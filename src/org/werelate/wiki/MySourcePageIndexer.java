package org.werelate.wiki;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class MySourcePageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_SURNAME, "mysource/surname"),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "mysource/place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_SURNAME_STORED, "mysource/surname"),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "mysource/place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_AUTHOR, "mysource/author"),
      new IndexInstruction(Utils.FLD_FROM_YEAR, "mysource/from_year"),
      new IndexInstruction(Utils.FLD_TO_YEAR, "mysource/to_year"),
   };

   public MySourcePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "mysource";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }
}
