package org.werelate.search;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.werelate.util.Utils;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;
import java.util.logging.Logger;
import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 30, 2008
 */
public class MultiRequestHandler extends RequestHandlerBase
{
   private static Logger logger = Logger.getLogger("org.werelate.search");

   private static final String QT_PLACE_INDEX = "placeindex";
   private static final String QT_PLACE_STANDARDIZE = "placestandardize";
   private static final String QT_PLACE_AUTO_COMPLETE = "placeautocomplete";
   private static final String QT_PLACE_LAT_LNG = "placelatlng";
   private static final String QT_PLACE_LAT_LNG_TITLE = "placelatlngtitle";
   private static final String QT_STATS = "stats";
   private static final String QT_REFRESH_STATS = "refreshstats";
   private static final String STATS_CACHE_NAME = "stats";
   private static final String MEMCACHE_STATS_KEY = "stats";
   private static final int MEMCACHE_EXPIRATION = 100000;

   // TODO someday change this to a solr generic cache?
   private static JCS statsCache;
   private static synchronized void ensureCache()
   {
      if (statsCache == null)
      {
         try
         {
            statsCache = JCS.getInstance(STATS_CACHE_NAME);
         } catch (CacheException e)
         {
            throw new RuntimeException("Couldn't instantiate cache: " + e.getMessage());
         }
      }
   }

   private static MemcachedClient memcache = null;
   private static class DaemonBinaryConnectionFactory extends BinaryConnectionFactory {
      @Override
      public boolean isDaemon() {
         return true;
      }
   }
   private static synchronized void ensureMemcache() {
      if (memcache == null) {
         try {
            String memcacheAddresses = System.getenv("memcache_address");
            memcache = new MemcachedClient(new DaemonBinaryConnectionFactory(),
                                           AddrUtil.getAddresses(memcacheAddresses));
         } catch (IOException e) {
            logger.warning("Unable to initialize memcache client");
         }
      }
   }

   public MultiRequestHandler() {
      super();
      ensureCache();
      ensureMemcache();
   }

   @SuppressWarnings("unchecked")
   private void handleIndexQuery(PlaceSearcher placeSearcher, String query, NamedList nl)
   {
      String[] ir = placeSearcher.getIndex(query);
      if (ir != null && ir.length > 0) {
         nl.add("index", ir);
      }
   }

   @SuppressWarnings("unchecked")
   private void handleStandardizeQuery(PlaceSearcher placeSearcher, String query, String defaultCountry, NamedList nl)
   {
      String[] matches = placeSearcher.standardize(query, defaultCountry);
      if (matches != null && matches.length > 0) {
         nl.add(Utils.FLD_PLACE_TITLE, matches[0]);
         if (matches.length > 1 && matches[1] != null) {
            nl.add("error", matches[1]);
         }
      }
   }

   @SuppressWarnings("unchecked")
   private void handleAutoCompleteQuery(PlaceSearcher placeSearcher, String query, NamedList nl)
   {
      if (query.endsWith(",")) {
         query = query.substring(0, query.length()-1);
      }
      else {
         query = query + "*";
      }
      String[][] acrs = placeSearcher.getAutoComplete(query);
      if (acrs != null && acrs.length > 0) {
         NamedList[] places = new NamedList[acrs.length];
         int j = 0;
         for (String[] acr : acrs) {
            NamedList nlp = new SimpleOrderedMap();
            if (acr.length > 0) {
               nlp.add(Utils.FLD_PLACE_TITLE, acr[0]);
            }
            if (acr.length > 1) {
               nlp.add(Utils.FLD_PLACE_TYPE, acr[1]);
            }
            places[j++] = nlp;
         }
         nl.add("places", places);
      }
   }

   @SuppressWarnings("unchecked")
   private void handleLatLngTitleQuery(PlaceSearcher placeSearcher, String query, NamedList nl)
   {
      float[] latlng = placeSearcher.getLatLng(query, true);
      if (latlng[0] != PlaceSearcher.MISSING_LATLNG && latlng[1] != PlaceSearcher.MISSING_LATLNG) {
         nl.add(Utils.FLD_LATITUDE, latlng[0]);
         nl.add(Utils.FLD_LONGITUDE, latlng[1]);
      }
   }

   @SuppressWarnings("unchecked")
   private void handleLatLngQuery(PlaceSearcher placeSearcher, String query, NamedList nl)
   {
      float[] latlng = placeSearcher.getLatLng(query, false);
      if (latlng.length > 2) {
         nl.add("error", (int)latlng[2]);
      }
      if (latlng[0] != PlaceSearcher.MISSING_LATLNG && latlng[1] != PlaceSearcher.MISSING_LATLNG) {
         nl.add(Utils.FLD_LATITUDE, latlng[0]);
         nl.add(Utils.FLD_LONGITUDE, latlng[1]);
      }
   }

   private void refreshNamespaceStats(SolrIndexSearcher searcher) {
      try {
         String[][] stats = new String[Utils.MAIN_NAMESPACES.length][2];
         int i = 0;
         for (String ns : Utils.MAIN_NAMESPACES) {
            int freq = 0;
            freq = searcher.search(new TermQuery(new Term(Utils.FLD_NAMESPACE, ns)), Integer.MAX_VALUE).totalHits;
            stats[i][0] = ns;
            stats[i][1] = Integer.toString(freq);
            i++;
         }

         // store in memcache
         memcache.set(MEMCACHE_STATS_KEY, MEMCACHE_EXPIRATION, stats);
      }
      catch (IOException e)
      {
         logger.warning("Couldn't get stats for namespace: " + e.getMessage());
      }
   }

   @SuppressWarnings("unchecked")
   private int getNamespaceStats(SolrIndexSearcher searcher, NamedList nl)
   {
      int totalCount = 0;

      // try reading stats from memcache; if not found, ignore
      String[][] stats = (String[][]) memcache.get(MEMCACHE_STATS_KEY);
      if (stats != null) {
         NamedList res = new SimpleOrderedMap();
         for (String[] nsFreq: stats) {
            int count = Integer.parseInt(nsFreq[1]);
            res.add(nsFreq[0], count);
            totalCount += count;
         }

         nl.add("Namespace", res);
      }

      return totalCount;
   }

   @SuppressWarnings("unchecked")
   private void getWatchlistStats(SolrIndexSearcher searcher, String userName, NamedList nl)
   {
      try
      {
         Integer count =  (Integer) statsCache.get(userName);

         if (count == null) {
            int freq = searcher.search(new TermQuery(new Term(Utils.FLD_USER, userName)), Integer.MAX_VALUE).totalHits;
            count = new Integer(freq);

            try
            {
               statsCache.put(userName, count);
            } catch (CacheException e)
            {
               logger.warning("Couldn't add to statsCache userName=" + userName + ": " + e.getMessage());
            }
         }

         nl.add("User:"+userName, count.intValue());
      }
      catch (IOException e)
      {
         logger.warning("Couldn't get stats for userName=" + userName + ": " + e.getMessage());
      }
   }


   @SuppressWarnings("unchecked")
   private void handleStatsQuery(SolrIndexSearcher searcher, String query, NamedList nl)
   {
      NamedList stats = new SimpleOrderedMap();
      NamedList temp = new SimpleOrderedMap();
      int totalCount = getNamespaceStats(searcher, temp);
      stats.add("facet_fields", temp);
      if (!Utils.isEmpty(query)) {
         temp = new SimpleOrderedMap();
         getWatchlistStats(searcher, query, temp);
         stats.add("facet_queries", temp);
      }
      nl.add("facet_counts", stats);
      nl.add("total", totalCount);
   }

   @Override
   @SuppressWarnings("unchecked")
   public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception, ParseException, InstantiationException, IllegalAccessException
   {
      PlaceSearcher placeSearcher = new PlaceSearcher(req.getSearcher(), req.getSchema().getQueryAnalyzer());

      // get query parameters
      SolrParams params = req.getParams();
      String queryString = params.get( CommonParams.Q );
      String defaultCountry = params.get("defaultCountry");
      String queryType = params.get( CommonParams.QT );
      NamedList[] result;

      if (queryString == null) { // this is valid for a stats query
         queryString = new String();
      }

      String[] queries = queryString.split("\\|");
      result = new NamedList[queries.length];
      int i = 0;
      for (String query : queries) {
         NamedList nl = new SimpleOrderedMap();
         nl.add("q", query);
         if (QT_PLACE_INDEX.equals(queryType)) {
            handleIndexQuery(placeSearcher, query, nl);
         }
         else if (QT_PLACE_STANDARDIZE.equals(queryType)) {
            handleStandardizeQuery(placeSearcher, query, defaultCountry, nl);
         }
         else if (QT_PLACE_AUTO_COMPLETE.equals(queryType)) {
            handleAutoCompleteQuery(placeSearcher, query, nl);
         }
         else if (QT_PLACE_LAT_LNG.equals(queryType)) {
            handleLatLngQuery(placeSearcher, query, nl);
         }
         else if (QT_PLACE_LAT_LNG_TITLE.equals(queryType)) {
            handleLatLngTitleQuery(placeSearcher, query, nl);
         }
         else if (QT_STATS.equals(queryType)) {
            handleStatsQuery(req.getSearcher(), query, nl);
         }
         else if (QT_REFRESH_STATS.equals(queryType)) {
            refreshNamespaceStats(req.getSearcher());
         }
         result[i++] = nl;
      }

      rsp.add("response", result);
   }

   //////////////////////// SolrInfoMBeans methods //////////////////////

   public String getDescription() {
      return "Multi request handler";
   }

   public String getVersion() {
      return "$Revision$";
   }

   public String getSourceId() {
      return "$Id: MultiRequestHandler.java $";
   }

   public String getSource() {
      return "$URL:  $";
   }
}
