package org.werelate.indexer;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by IntelliJ IDEA.
 * User: dallan
 */
public class TitleSorter {
   private PreparedStatement psGetPrev;
   private PreparedStatement psGetNext;
   private PreparedStatement psInsert;

   public TitleSorter(DatabaseConnectionHelper conn) throws SQLException {
      // TODO title_sort
      this.psGetPrev = conn.preparedStatement("SELECT sort_key, value FROM title_sort_key where sort_key <= ? ORDER BY sort_key desc limit 1");
      this.psGetNext = conn.preparedStatement("SELECT value FROM title_sort_key where sort_key > ? ORDER BY sort_key asc limit 1");
      this.psInsert = conn.preparedStatement("INSERT IGNORE INTO title_sort_key VALUES (?, ?)");
   }

   // keep in sync with TitleSorter in indexer project
   private String generateSortKey(String title) {
      title = Utils.romanize(title);
      if (title.length() > 80) {
         title = title.substring(0, 80);
      }
      return title.toLowerCase().trim();
   }

   public int getSortValue(String title) throws SQLException {
      int value;
      // generate the sort key
      String key = generateSortKey(title);

      // get the sort value <=
      String prevKey = "";
      int prevValue = Integer.MIN_VALUE;
      psGetPrev.setString(1, key);
      ResultSet rs = psGetPrev.executeQuery();
      if (rs.next()) {
         prevKey = rs.getString(1);
         prevValue = rs.getInt(2);
      }
      rs.close();

      // if the keys are equal, use the value
      if (key.equals(prevKey)) {
         value = prevValue;
      }
      // if keys aren't equal, get the next sort value >
      else {
         int nextValue = Integer.MAX_VALUE;
         psGetNext.setString(1, key);
         rs = psGetNext.executeQuery();
         if (rs.next()) {
            nextValue = rs.getInt(1);
         }
         rs.close();
         // the value is the midway point
         value = (int)(((long)prevValue + (long)nextValue)/(long)2);

         // insert the new sort value for this title
         if (value != prevValue) {
            psInsert.setString(1, key);
            psInsert.setInt(2, value);
            psInsert.executeUpdate();
         }
      }

      return value;
   }
}
