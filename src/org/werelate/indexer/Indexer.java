package org.werelate.indexer;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.File;
import java.sql.SQLException;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.cli.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.HttpClientHelper;
import org.werelate.util.Utils;
import org.werelate.wiki.*;
import nu.xom.*;

/**
 * Created by Dallan Quass
 * Date: Apr 18, 2008
 */
public class Indexer
{
   private static final Logger logger = Logger.getLogger("org.werelate.wiki");
   private static final String PLACE_REDIR_CACHE_NAME = "placeRedirect";

   private Properties properties;
   private long stopTimeMillis;
   private int foregroundDelayMillis;
   private int backgroundDelayMillis;
   private int maxBackgroundPages;
   private int maxIndexTaskSize;
   private int indexBatchSize;
   private Map<String,Integer> indexedRevisions;
   private DatabaseConnectionHelper conn;
   private HttpClientHelper wikiClient;
   private String wikiHostname;
   private String indexUrl;
   private SolrServer solr;
   private MemcachedClient memcache;
   private PlaceStandardizer placeStandardizer;
   private CheckpointManager irCm;
   private CheckpointManager revCm;
   private CheckpointManager mlCm;
   private CheckpointManager dlCm;
   private CheckpointManager apCm;
   private BasePageIndexer articlePageIndexer;
   private BasePageIndexer transcriptPageIndexer;
   private BasePageIndexer userPageIndexer;
   private BasePageIndexer imagePageIndexer;
   private BasePageIndexer givennamePageIndexer;
   private BasePageIndexer surnamePageIndexer;
   private BasePageIndexer placePageIndexer;
   private BasePageIndexer sourcePageIndexer;
   private BasePageIndexer mysourcePageIndexer;
   private BasePageIndexer repositoryPageIndexer;
   private BasePageIndexer personPageIndexer;
   private BasePageIndexer personTalkPageIndexer;
   private BasePageIndexer familyPageIndexer;
   private BasePageIndexer familyTalkPageIndexer;
   private BasePageIndexer categoryPageIndexer;
   private BasePageIndexer defaultPageIndexer;

   private Indexer(Properties p) {
      this.properties = p;
      this.conn = null;
      this.memcache = null;
   }

   private void startIndexing() throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
      // calc when we should stop
      int millis = Integer.parseInt(properties.getProperty("max_index_seconds")) * 1000;
      stopTimeMillis = millis > 0 ? System.currentTimeMillis() + millis : 0;
      indexedRevisions = new HashMap<String,Integer>();
      foregroundDelayMillis = Integer.parseInt(properties.getProperty("foreground_delay_millis"));
      backgroundDelayMillis = Integer.parseInt(properties.getProperty("background_delay_millis"));
      maxBackgroundPages = Integer.parseInt(properties.getProperty("max_background_pages"));
      maxIndexTaskSize = Integer.parseInt(properties.getProperty("max_index_task_size"));
      indexBatchSize = Integer.parseInt(properties.getProperty("index_batch_size"));
      indexUrl = properties.getProperty("index_url");
      wikiHostname = properties.getProperty("wiki_hostname");

      // get a database connection
      conn = new DatabaseConnectionHelper(properties.getProperty("db_url"),
                                          properties.getProperty("db_username"), properties.getProperty("db_passwd"));
      conn.connect();

      // Set up http client helpers
      wikiClient = new HttpClientHelper(true);

      // Set up solr connection
      solr = new CommonsHttpSolrServer(indexUrl);

      // Set up redir cache
      String memcacheAddress = properties.getProperty("memcache_address");
      memcache = new MemcachedClient(new BinaryConnectionFactory(),
                                     AddrUtil.getAddresses(memcacheAddress));

      // Set up place standardizer
      placeStandardizer = new PlaceStandardizer(indexUrl, memcache, wikiHostname, wikiClient);

      // Set up checkpoint managers
      irCm = new CheckpointManager(conn, "index_requests");
      revCm = new CheckpointManager(conn, "revisions");
      mlCm = new CheckpointManager(conn, "move_log");
      dlCm = new CheckpointManager(conn, "delete_log");
      apCm = new CheckpointManager(conn, "all_pages");

      // create page indexers
      articlePageIndexer = new ArticlePageIndexer(conn);
      transcriptPageIndexer = new TranscriptPageIndexer(conn);
      userPageIndexer = new UserPageIndexer(conn);
      imagePageIndexer = new ImagePageIndexer(conn);
      givennamePageIndexer = new GivennamePageIndexer(conn);
      surnamePageIndexer = new SurnamePageIndexer(conn);
      placePageIndexer = new PlacePageIndexer(conn);
      sourcePageIndexer = new SourcePageIndexer(conn);
      mysourcePageIndexer = new MySourcePageIndexer(conn);
      repositoryPageIndexer = new RepositoryPageIndexer(conn);
      personPageIndexer = new PersonPageIndexer(conn);
      personTalkPageIndexer = new PersonTalkPageIndexer(conn);
      familyPageIndexer = new FamilyPageIndexer(conn);
      familyTalkPageIndexer = new FamilyTalkPageIndexer(conn);
      categoryPageIndexer = new CategoryPageIndexer(conn);
      defaultPageIndexer = new DefaultPageIndexer(conn);
   }

   private void cleanup()
   {
      logger.info("Cleaning up");
      // close the database connection
      if (conn != null) {
         conn.close();
      }
      logger.info("Database connection closed");
      if (memcache != null) {
         memcache.shutdown();
      }
      logger.info("memcache connection closed");
   }

   private SolrInputDocument indexPage(String pageId, int ns, String fullTitle, String lastModDate, int popularity, String contents, List<String> users, List<String> trees) throws SQLException
   {
      BasePageIndexer indexer = null;

//      logger.info("Indexing pageId=" + pageId + " ns=" + ns + " fullTitle=" + fullTitle + " lastModDate=" + lastModDate);

      switch (ns) {
         case Utils.NS_MAIN:
            indexer = articlePageIndexer;
            break;
         case Utils.NS_TRANSCRIPT:
            indexer = transcriptPageIndexer;
            break;
         case Utils.NS_USER:
            indexer = userPageIndexer;
            break;
         case Utils.NS_IMAGE:
            indexer = imagePageIndexer;
            break;
         case Utils.NS_GIVENNAME:
            indexer = givennamePageIndexer;
            break;
         case Utils.NS_SURNAME:
            indexer = surnamePageIndexer;
            break;
         case Utils.NS_PLACE:
            indexer = placePageIndexer;
            break;
         case Utils.NS_SOURCE:
            indexer = sourcePageIndexer;
            break;
         case Utils.NS_MYSOURCE:
            indexer = mysourcePageIndexer;
            break;
         case Utils.NS_REPOSITORY:
            indexer = repositoryPageIndexer;
            break;
         case Utils.NS_PERSON:
            indexer = personPageIndexer;
            break;
         case Utils.NS_PERSON + 1:
            indexer = personTalkPageIndexer;
            break;
         case Utils.NS_FAMILY:
            indexer = familyPageIndexer;
            break;
         case Utils.NS_FAMILY + 1:
            indexer = familyTalkPageIndexer;
            break;
         case Utils.NS_CATEGORY:
            indexer = categoryPageIndexer;
            break;
         default:
            indexer = defaultPageIndexer;
      }
      return indexer.index(pageId, ns, fullTitle, lastModDate, popularity, contents, users, trees);
   }

   private void indexBatch(List<IndexTask> indexBatch) throws IOException, ParsingException, SolrServerException, SQLException {
      logger.info("Indexing batch size="+indexBatch.size()+" first sequenceId="+indexBatch.get(0).getSequenceId());

      StringBuilder pageIds = new StringBuilder();
      for (IndexTask it : indexBatch) {
         if (it.getAction() == IndexTask.ACTION_DELETE) {
//            logger.info("  deleting "+it.getPageId());
            solr.deleteById(it.getPageId());
         }
         else {
            if (pageIds.length() > 0) {
               pageIds.append(",");
            }
            pageIds.append(it.getPageId());
         }
      }

      if (pageIds.length() > 0) {
         Map<String,String> args = new HashMap<String,String>();
         args.put("page_id", pageIds.toString());
         args.put("index", "t");
         GetMethod m = new GetMethod(Utils.getWikiAjaxUrl(wikiHostname, "wfGetPageIndexContents", args));
         Elements pages = null;
         try
         {
            wikiClient.executeHttpMethod(m);
            int statusCode = m.getStatusCode();
            if (statusCode != 200) {
               throw new RuntimeException("Unexpected http status code="+statusCode+" for pages="+pageIds.toString());
            }
            String response=HttpClientHelper.getResponse(m);
            if (Utils.isEmpty(response)) {
               throw new RuntimeException("Unexpected empty response for pages="+pageIds.toString());
            }
            Element root = wikiClient.parseText(response).getRootElement();
            if (Integer.parseInt(root.getAttributeValue("status")) != HttpClientHelper.STATUS_OK) {
               throw new RuntimeException("Unexpected status="+root.getAttributeValue("status")+" for pages="+pageIds.toString());
            }
            pages = root.getChildElements("page");
         }
         finally
         {
            m.releaseConnection();
         }

         // index each returned page
         List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
         List<String> userList = new ArrayList<String>();
         List<String> treeList = new ArrayList<String>();
         for (int i=0; i < pages.size(); i++)
         {
            Element page = pages.get(i);
            String pageId = page.getAttributeValue("page_id");
            String fullTitle = page.getAttributeValue("title");
            if (Utils.isEmpty(fullTitle)) {
               logger.warning("Empty page="+pageId);
               continue;
            }

            Elements users = page.getChildElements("user");
            userList.clear();
            for (int u=0; u < users.size(); u++) {
               Element user = users.get(u);
               userList.add(user.getValue());
            }
            Elements trees = page.getChildElements("tree");
            treeList.clear();
            for (int t=0; t < trees.size(); t++) {
               Element tree = trees.get(t);
               treeList.add(tree.getValue());
            }
            int revId = Integer.parseInt(page.getAttributeValue("rev_id"));

            // if we haven't already indexed this revision (check again because not all generators return revisions)
            Integer revisionId = indexedRevisions.get(pageId);
            int ns = Integer.parseInt(page.getAttributeValue("namespace"));
            int popularity = 0;
            String popularityString = page.getAttributeValue("popularity");
            if (popularityString.length() > 0) popularity = Integer.parseInt(popularityString);
            if (revisionId == null || revisionId < revId) {
               SolrInputDocument doc = indexPage(pageId, ns, fullTitle, page.getAttributeValue("rev_timestamp"), popularity,
                                                 page.getFirstChildElement("contents").getValue(), userList, treeList);
               if (doc != null) {
//                  logger.info("  indexing page "+pageId);
                  docs.add(doc);
               }
               else { // could be a redirect that we're no longer indexing
//                  logger.info("  deleting redirect "+pageId);
                  if (ns == Utils.NS_PLACE) {
                     memcache.delete(Utils.getMemcacheKey(PlaceStandardizer.MC_PREFIX, fullTitle)); // remove title from cache
                  }
                  solr.deleteById(pageId);
               }
               // remember indexing this revision
               indexedRevisions.put(pageId, revId);
            }
         }

         if (docs.size() > 0) {
//            logger.info("  sending " + docs.size() + " docs to solr");
            placeStandardizer.standardizePlaces(docs);
            solr.add(docs);
         }
      }
   }

   private boolean index(BaseTaskGenerator itg, CheckpointManager cm, int delayMillis, int maxPages) throws IOException, SQLException, ParsingException, SolrServerException {
      List<IndexTask> indexTasks;
      Set<String> seenPageIds = new HashSet<String>();
      List<IndexTask> indexBatch = new ArrayList<IndexTask>();
      int max;
      boolean keepIndexing = true;
      int pagesIndexed = 0;
      if (delayMillis == 0) delayMillis = 1;

      do {
         max = stopTimeMillis > 0 ? Math.max(1, Math.min(maxIndexTaskSize, (int)((stopTimeMillis - System.currentTimeMillis()) / delayMillis))) : maxIndexTaskSize;
         if (maxPages > 0 && max + pagesIndexed > maxPages) {
            max = Math.max(1, maxPages - pagesIndexed);
         }
         logger.info("  requesting "+ max + " index tasks");
         indexTasks = itg.getTasks(cm, max);
         logger.info("  got "+ indexTasks.size() + " index tasks starting with sequenceId="+(indexTasks.size() == 0 ? "" : indexTasks.get(0).getSequenceId()));

         for (IndexTask it : indexTasks) {
            if (seenPageIds.contains(it.getPageId())) {
               // skip because we've already seen this page id from this generator
//               logger.info("  already seen " + it.getPageId());
            }
            else if (it.getRevId() > 0 && indexedRevisions.get(it.getPageId()) != null && indexedRevisions.get(it.getPageId()) >= it.getRevId()) {
               // skip because we've already indexed this or a later revision
//               logger.info("  already indexed " + it.getPageId());
            }
            else {
               // enqueue this page for indexing
//               logger.info("  enqueue " + it.getPageId());
               indexBatch.add(it);
            }
            seenPageIds.add(it.getPageId());

            // set the checkpoint
            itg.updateCheckpoint(it, cm);

            if (indexBatch.size() >= indexBatchSize) {
               // index the pages
               long restartMillis = System.currentTimeMillis() + indexBatch.size() * delayMillis;
               indexBatch(indexBatch);
               indexBatch.clear();

               // wait between each batch
               long currTimeMillis = System.currentTimeMillis();
               if (currTimeMillis < restartMillis && (restartMillis < stopTimeMillis || stopTimeMillis == 0)) {
                  Utils.sleep((int)(restartMillis - currTimeMillis));
               }

               // if we've run out of time, stop
               if (stopTimeMillis > 0 && (restartMillis >= stopTimeMillis || System.currentTimeMillis() >= stopTimeMillis)) {
                  keepIndexing = false;
                  break;
               }
            }
         }
         pagesIndexed += indexTasks.size();

         if (max < maxIndexTaskSize && indexTasks.size() == max) { // we requested < MAX and we've indexed that many
            keepIndexing = false;
         }

      } while (keepIndexing && indexTasks.size() == maxIndexTaskSize && (maxPages == 0 || pagesIndexed < maxPages));

      if (indexBatch.size() > 0) {
         // index the pages
         indexBatch(indexBatch);
      }

      return keepIndexing;
   }

   public void commit() throws IOException, SQLException, SolrServerException
   {
      logger.info("Committing index");
      commitWithTimeout();

      logger.info("Saving checkpoints");
      irCm.saveCheckpoint();
      revCm.saveCheckpoint();
      mlCm.saveCheckpoint();
      dlCm.saveCheckpoint();
      apCm.saveCheckpoint();
   }

   public void commitWithTimeout()
   {
      ExecutorService executor = Executors.newCachedThreadPool();
      Callable<Object> task = new Callable<Object>() {
         public Object call() throws ExecutionException {
            try {
               return solr.commit();
            } catch (IOException e) {
               logger.info("ERROR IOException: " + e);
               throw new ExecutionException(e);
            } catch (SolrServerException e) {
               logger.info("ERROR SolrServerException: " + e);
               throw new ExecutionException(e);
            }
         }
      };
      Future<Object> future = executor.submit(task);
      try {
         Object result = future.get(30, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
         logger.info("ERROR Commit timeout exception: " + e);
         System.exit(1);
      } catch (InterruptedException e) {
         logger.info("ERROR Commit interrupted exception: " + e);
         System.exit(1);
      } catch (ExecutionException e) {
         logger.info("ERROR Commit execution error: " + e);
         System.exit(1);
      } finally {
         future.cancel(true); // may or may not desire this
      }
   }

   public void indexChanges() throws IOException, SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException, ParsingException, SolrServerException {
      boolean indexingStarted = false;
      try {
         startIndexing();
         boolean doIndexing = true;
         indexingStarted = true;

         // the tasks need to be done in this order
         // index renames
         if (doIndexing) {
            logger.info("Indexing renames");
            doIndexing = index(new MoveLogTaskGenerator(wikiClient, wikiHostname, memcache), mlCm, foregroundDelayMillis, 0);
         }
         // index index-requests
         if (doIndexing) {
            logger.info("Indexing requests");
            doIndexing = index(new IndexRequestTaskGenerator(wikiClient, wikiHostname), irCm, foregroundDelayMillis, 0);
         }
         // index new reversions
         if (doIndexing) {
            logger.info("Indexing revisions");
            doIndexing = index(new RevisionTaskGenerator(wikiClient, wikiHostname), revCm, foregroundDelayMillis, 0);
         }
         // index deletes and undeletes
         if (doIndexing) {
            logger.info("Indexing deletes and undeletes");
            doIndexing = index(new DeleteLogTaskGenerator(wikiClient, wikiHostname), dlCm, foregroundDelayMillis, 0);
         }
         // re-index all pages if still time
         if (doIndexing && maxBackgroundPages > 0) {
            logger.info("Indexing all pages:" + maxBackgroundPages);
            // index may return false because maxBackgroundPages have been indexed
            doIndexing = index(new AllPagesTaskGenerator(wikiClient, wikiHostname), apCm, backgroundDelayMillis, maxBackgroundPages);
         }
      }
      finally {
         if (indexingStarted) {
            commit(); // commit the docs we've sent even if this client crashed
         }
         cleanup();
         logger.info("Deleting inprocess file");
      }
   }

   private void indexNamespaces(String[] namespaces, String startingPageId) throws IOException, SQLException, ParsingException, SolrServerException {
      apCm.updateCheckpoint(startingPageId);
      AllPagesTaskGenerator aptg = new AllPagesTaskGenerator(wikiClient, wikiHostname, false, namespaces);
      do {
         index(aptg, apCm, backgroundDelayMillis, maxBackgroundPages);
         commitWithTimeout();
         logger.info("committed index page_id="+apCm.getCheckpoint());
      } while (!aptg.isAtEnd());
   }

   public void indexAll() throws IOException, SolrServerException, ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, ParsingException {
      AllPagesTaskGenerator aptg;
      boolean commitNeeded = false;
      try {
         startIndexing();

         // index given names, surnames, and places first
//         logger.info("Indexing givennames, surnames, places");
//         String[] namespaces = {"100","102","106"};
//         commitNeeded = true;
//         indexNamespaces(namespaces, "0");
//         commitNeeded = false;

         // index everything
         logger.info("Indexing everything");
         commitNeeded = true;
         indexNamespaces(null, "0");
         commitNeeded = false;
      }
      finally {
         if (commitNeeded) {
            logger.info("Committing index");
            commitWithTimeout();
         }
         cleanup();
      }
      logger.info("IndexAll complete");
   }

   public static void main(String[] args) throws ParseException, IOException, IllegalAccessException, SQLException, ParsingException, ClassNotFoundException, InstantiationException, SolrServerException {
      Options opt = new Options();
      opt.addOption("p", true, "java .properties file");
      opt.addOption("r", false, "if set, re-index everything; set mergeFactor in solrconfig.xml to a high number like 20 for this option");
      opt.addOption("h", false, "Print out help information");

      BasicParser parser = new BasicParser();
      CommandLine cl = parser.parse(opt, args);
      if (cl.hasOption("h") || !cl.hasOption("p"))
      {
          System.out.println("Index wiki pages to SOLR server.");
          HelpFormatter f = new HelpFormatter();
          f.printHelp("OptionsTip", opt);
      }
      // read parameters and calculate stop time
      Properties properties = new Properties();
      properties.load(new FileInputStream(cl.getOptionValue("p")));
      Indexer indexer = new Indexer(properties);
      if (cl.hasOption("r")) {
         indexer.indexAll();
      }
      else {
         indexer.indexChanges();
      }
   }
}
