package org.werelate.wiki;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;
import org.apache.solr.common.SolrInputDocument;

import java.sql.SQLException;

import nu.xom.Document;

/**
 * Created by Dallan Quass
 * Date: May 8, 2008
 */
public class UserPageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_SURNAME, "user/researching/@surname"),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "user/researching/@place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_SURNAME_STORED, "user/researching/@surname"),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "user/researching/@place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
   };

   public UserPageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "user";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      doc.addField(Utils.FLD_USER, title);
   }
}
