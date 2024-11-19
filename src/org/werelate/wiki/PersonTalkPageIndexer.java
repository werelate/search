package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.folg.names.search.Normalizer;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nu.xom.Document;
import nu.xom.Nodes;
import nu.xom.Element;

/**
 * Created by Janet Bjorndahl
 * Date: Jul 18, 2024
 */
public class PersonTalkPageIndexer extends BasePageIndexer
{

   private static final IndexInstruction[] INSTRUCTIONS = {
   };

   private String pageSortName = null;

   public PersonTalkPageIndexer(DatabaseConnectionHelper conn) throws SQLException, IOException {
      super(conn);
   }

   /* No need to parse the XML - all the info comes from the page title. */
   protected String getTagName() {
      return "";
   }
 
   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   /* Override base method. Note that this also stores the sort name for use in creating the surname index facet. */
   protected String getTitleSort(String fullTitle, Document xml) {
      pageSortName = PersonPageIndexer.getReversedTitle(fullTitle.substring(12));
      return "Person talk:" + pageSortName;
   }

   // add display name, surname index facet
   // Note: This relies on getTitleSort being run first. 
   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      doc.addField(Utils.FLD_FULLNAME_STORED, pageSortName);
      doc.addField(Utils.FLD_SURNAME_INDEX_FACET, PersonPageIndexer.getSurnameIndex(pageSortName));
   }
}
