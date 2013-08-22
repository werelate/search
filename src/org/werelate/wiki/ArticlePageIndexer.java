package org.werelate.wiki;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class ArticlePageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_SURNAME, "article/surname"),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "article/place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_SURNAME_STORED, "article/surname"),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "article/place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_FROM_YEAR, "article/from_year"),
      new IndexInstruction(Utils.FLD_TO_YEAR, "article/to_year"),
   };

   public ArticlePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "article";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }
}
