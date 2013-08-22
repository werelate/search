package org.werelate.indexer;

import org.werelate.util.DatabaseConnectionHelper;

import java.io.*;
import java.sql.*;

/**
 * Created by Dallan Quass
 * Date: Apr 18, 2008
 */
public class CheckpointManager
{
   private String name;
   private String checkpoint;
   private String revTimestamp;
   private boolean changed;
   private PreparedStatement psRead;
   private PreparedStatement psUpdate;

   public CheckpointManager(DatabaseConnectionHelper conn, String name) throws SQLException
   {
      changed = false;
      this.name = name;
      this.psRead = conn.preparedStatement("SELECT ic_checkpoint, ic_rev_timestamp FROM index_checkpoint WHERE ic_name = ?");
      this.psUpdate = conn.preparedStatement("UPDATE index_checkpoint SET ic_checkpoint = ?, ic_rev_timestamp = ? WHERE ic_name = ?");
      this.checkpoint = null;
      this.revTimestamp = null;
   }

   public String getCheckpoint() throws SQLException
   {
      if (checkpoint == null) {
         // read from database
         psRead.setString(1, name);
         ResultSet rs = psRead.executeQuery();
         if(rs.next()) {
            checkpoint = rs.getString(1);
            revTimestamp = rs.getString(2);
         }
         else {
            checkpoint = "0";
         }
         rs.close();
      }
      return checkpoint;
   }

   public void updateCheckpoint(String checkpoint) {
      updateCheckpoint(checkpoint, revTimestamp);
   }

   public void updateCheckpoint(String checkpoint, String revTimestamp)
   {
      this.checkpoint = checkpoint;
      this.revTimestamp = revTimestamp;
      this.changed = true;
   }

   public void saveCheckpoint() throws IOException, SQLException
   {
      // write to database
      if (changed) {
         psUpdate.setString(1, checkpoint);
         psUpdate.setString(2, revTimestamp);
         psUpdate.setString(3, name);
         psUpdate.executeUpdate();
         changed = false;
      }
   }
}
