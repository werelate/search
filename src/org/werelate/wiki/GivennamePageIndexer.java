package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.util.List;
import java.util.Map;
import java.sql.SQLException;

import nu.xom.Document;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class GivennamePageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_RELATED_NAME, "givenname/related/@name"),
   };

   public GivennamePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "givenname";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   // add givenname from title
   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      doc.addField(Utils.FLD_OTHER_GIVENNAME, title);
      doc.addField(Utils.FLD_GIVENNAME_TITLE, title);
   }
}
