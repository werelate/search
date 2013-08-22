package org.werelate.wiki;

import org.werelate.util.DatabaseConnectionHelper;

import java.sql.SQLException;

/**
 * Created by Dallan Quass
 * Date: May 8, 2008
 */
public class DefaultPageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
   };

   public DefaultPageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return null;
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }
}
