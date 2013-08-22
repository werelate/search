package org.werelate.wiki;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

/**
 * Created by Dallan Quass
 */
public class TranscriptPageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_SURNAME, "transcript/surname"),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "transcript/place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_SURNAME_STORED, "transcript/surname"),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "transcript/place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_FROM_YEAR, "transcript/from_year"),
      new IndexInstruction(Utils.FLD_TO_YEAR, "transcript/to_year"),
   };

   public TranscriptPageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "transcript";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }
}
