package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

import nu.xom.Document;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class SurnamePageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_RELATED_NAME, "surname/related/@name"),
   };

   public SurnamePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "surname";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   // add surname from title
   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      doc.addField(Utils.FLD_OTHER_SURNAME, title);
      doc.addField(Utils.FLD_SURNAME_TITLE, title);
   }
}
