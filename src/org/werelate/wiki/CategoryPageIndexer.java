package org.werelate.wiki;

import org.werelate.util.Utils;
import org.werelate.util.DatabaseConnectionHelper;
import org.apache.solr.common.SolrInputDocument;

import java.sql.SQLException;

import nu.xom.Document;

/**
 * Created by Dallan Quass
 * Date: Jul 1, 2008
 */
public class CategoryPageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_SURNAME, "category/surname"),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "category/place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_SURNAME_STORED, "category/surname"),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "category/place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
   };

   public CategoryPageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "category";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      doc.addField(Utils.FLD_CATEGORY, title);
   }
}
