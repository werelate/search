package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.werelate.indexer.TitleSorter;
import org.werelate.util.Utils;
import org.werelate.util.DatabaseConnectionHelper;

import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.IOException;
import java.text.NumberFormat;
import java.sql.SQLException;

import nu.xom.*;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public abstract class BasePageIndexer
{
   protected static Logger logger = Logger.getLogger("org.werelate.wiki");
   private static final int MAX_INT_DIGITS = 10;
   private static final Pattern CATEGORY_PATTERN = Pattern.compile("\\[\\[[cC]ategory:([^\\|\\]]+).*?\\]\\]", Pattern.DOTALL);
   private static final Pattern REDIRECT_PATTERN = Pattern.compile("#redirect\\s*\\[\\[(.*?)\\]\\]", Pattern.CASE_INSENSITIVE);
   private static final Pattern INDEX_NUMBER_PATTERN = Pattern.compile("\\(\\d+\\)$");

   protected abstract String getTagName();
   protected abstract IndexInstruction[] getIndexInstructions();
   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      // nothing -- override to add additional fields
   }

   private Builder builder;
   private TitleSorter titleSorter;
   private NumberFormat nf;

   public BasePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      this.builder = new Builder();
      this.titleSorter = new TitleSorter(conn);
      this.nf = NumberFormat.getIntegerInstance();
      nf.setMinimumIntegerDigits(MAX_INT_DIGITS);
      nf.setGroupingUsed(false);
   }

   public SolrInputDocument index(String pageId, int ns, String fullTitle, String revTimestamp, int popularity, String contents, List<String> users, List<String>trees) throws SQLException
   {
      String[] namespaceTitle = Utils.splitNamespaceTitle(fullTitle); // return namespace text in field[0], title in field[1]
      String title = namespaceTitle[1];
      String namespace = namespaceTitle[0];

      SolrInputDocument doc = new SolrInputDocument();
      boolean redir = false;
      String redirTitle = null;

      Matcher m = REDIRECT_PATTERN.matcher(contents);
      if (m.lookingAt()) {
//         if (indexRedirects()) {
//            redirTitle = m.group(1);
//            doc.addField(Utils.FLD_REDIRECT, Utils.translateHtmlCharacterEntities(redirTitle));
//            redir = true;
//         }
//         else {
            return null;
//         }
      }

      doc.addField(Utils.FLD_TITLE_STORED, title); // FLD_TITLE is added below
      String mainNamespace = Utils.getMainNamespace(namespace);
      doc.addField(Utils.FLD_NAMESPACE, mainNamespace);
      doc.addField(Utils.FLD_NAMESPACE_STORED, namespace);
      if (namespace.endsWith(" talk") || namespace.equals("Talk")) {
         doc.addField(Utils.FLD_TALK_NAMESPACE, true);
      }
      doc.addField(Utils.FLD_LAST_MOD_DATE, revTimestamp);
      doc.addField(Utils.FLD_PAGE_ID, pageId);
      doc.addField(Utils.FLD_POPULARITY, popularity);
      doc.addField(Utils.FLD_KEYWORDS, fullTitle); // re-index full title under keywords so we don't have to do dis-max queries on keywords+title
      contents = contents.replace("<show_sources_images_notes/>",""); // no need to index/store this
      doc.addField(Utils.FLD_KEYWORDS, contents);

      Document xml = null;

      if (!redir) {
         m = CATEGORY_PATTERN.matcher(contents);
         while (m.find()) {
            doc.addField(Utils.FLD_CATEGORY, m.group(1));
         }

         for (String user : users) {
            doc.addField(Utils.FLD_USER, user);
         }

         for (String tree : trees) {
            doc.addField(Utils.FLD_TREE, tree);
         }

         // add namespace-specific fields
         String tagName = getTagName();
         if (!Utils.isEmpty(tagName)) {
            String[] fields = Utils.splitStructuredWikiText(getTagName(), contents);
            contents = fields[1];
            if (!Utils.isEmpty(fields[0])) {
               try
               {
                  xml = Utils.parseText(builder, fields[0], true);
                  IndexInstruction[] indexInstructions = getIndexInstructions();
                  for (IndexInstruction ii : indexInstructions) {
                     Nodes nodes = xml.query(ii.getXPath());
                     for (int i = 0; i < nodes.size(); i++) {
                        String val = nodes.get(i).getValue();
                        int filterAction = ii.getFilterAction();
                        if (filterAction == IndexInstruction.FILTER_REMOVE_LINK) {
                           if (val.startsWith("[[")) val = val.substring(2);
                           if (val.endsWith("]]")) val = val.substring(0, val.length()-2);
                           if (val.indexOf('|') < 0) {
                              val = val.replaceFirst("\\(\\d+\\)$", ""); // remove ending index number if there is one
                           }
                           else if (!val.endsWith("|")) {
                              filterAction = IndexInstruction.FILTER_REMOVE_PRE_BAR;
                           }
                        }
                        if (filterAction == IndexInstruction.FILTER_REMOVE_POST_BAR ||
                            filterAction == IndexInstruction.FILTER_REMOVE_PRE_BAR) {
                           int pos = val.indexOf('|');
                           if (pos >= 0) {
                              val = (filterAction == IndexInstruction.FILTER_REMOVE_POST_BAR ? val.substring(0, pos) : val.substring(pos+1));
                           }
                        }
                        doc.addField(ii.getFieldName(), val);
                     }
                     if (nodes.size() == 0 && ii.getDefaultValue() != null) {
                        doc.addField(ii.getFieldName(), ii.getDefaultValue());
                     }
                  }

               }
               catch (ParsingException e)
               {
                  logger.warning("Parsing exception: " + e);
               }
               catch (IOException e)
               {
                  logger.warning("IO exception: " + e);
               }
            }
         }

         // add contents, after the xml has been removed
         doc.addField(Utils.FLD_TEXT_STORED, contents);
      }

      // add title (getTitleIndex may be overridden)
      doc.addField(Utils.FLD_TITLE, getTitleIndex(title, xml));

      // add title sort value (getTitleSort may be overridden)
      doc.addField(Utils.FLD_TITLE_SORT_VALUE, titleSorter.getSortValue(getTitleSort(fullTitle, xml)));

      // add any other custom fields
      // Note that PersonPageIndexer and FamilyPageIndexer rely on this being run after getTItleSort
      addCustomFields(doc, title, xml, contents, redirTitle);

      return doc;
   }

   protected static String removeIndexNumber(String title) {
      return title.replaceAll("\\s*\\(\\d+\\)", "").trim();
   }

   protected String[][] getFamilyNamePieces(String titleSansIndexNumber) {
      String[][] familyNamePieces = new String[2][2];
      for (int i = 0; i < 2; i++) for (int j = 0; j < 2; j++) familyNamePieces[i][j] = null;

      String[] parents = titleSansIndexNumber.split(" and ", 2);
      for (int i = 0; i < parents.length; i++) {
         String[] namePieces = parents[i].split(" ", 2);
         for (int j = 0; j < namePieces.length; j++) {
            familyNamePieces[i][j] = namePieces[j];
         }
      }
      return familyNamePieces;
   }

   protected String getTitleIndex(String title, Document xml) {
      return title;
   }

   protected String getTitleSort(String title, Document xml) {
      return title;
   }

   private static final String XPATH_SOURCED = "//source_citation | //image | //note";
   protected boolean isUnsourced(Document xml, String contents) {
      Nodes nodes = xml.query(XPATH_SOURCED);
      return (contents == null || contents.replace("<show_sources_images_notes/>","").trim().length() == 0) && nodes.size() == 0; // unsourced if no source citations, images, or notes
   }
}
