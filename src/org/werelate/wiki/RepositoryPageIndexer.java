package org.werelate.wiki;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class RepositoryPageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "repository/place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "repository/place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
   };

   public RepositoryPageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "repository";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }
}
