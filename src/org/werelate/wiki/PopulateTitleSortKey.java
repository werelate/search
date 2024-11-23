/*
 * Copyright 2012 Foundation for On-Line Genealogy, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.werelate.wiki;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.*;
import java.sql.Connection;

/**
 * This class builds a new version of the data in title_sort_key to replace existing data in title_sort_key.
 * 
 * Its primary purpose is to be part of the Nov 2024 implementation of changes in sort order.
 * It can be used in the future if needed, but must first be reviewed to ensure that it is using up-to-date 
 * code for generating sort names. The execution plan must also be reviewed (see options below).
 * 
 * The rebuild of title_sort_key is done in 4 steps:
 *    Run GenerateSortNames to generate sort titles from pages.xml, placing results in temp_sort_name
 *    Run PopulateTitleSortKey to:
 *       De-duplicate sort titles, placing results in temp_title_sort_key
 *       Calculate new sort values in temp_title_sort_key
 *    Manually backup and truncate title_sort_key and replace with records from temp_title_sort_key
 * 
 * When running PopulateTitleSortKey, the user can choose to run one step at a time (to review intermediate results), or both steps in one run.
 * The user also has a choice of how large a gap to use between records for high-growth namespaces (e.g., Person, Family) relative to 
 * lower-growth namespaces (e.g., Categories, Templates). For the Nov 2024 implementation, this muliplier was set to 5.
 * 
 * Note that one of the results of running the steps above is that records in title_sort_key that are no longer relevant (deleted or renamed pages)
 * will no longer exist in that table.
 * 
 * Because the last step is a truncate and reload, there is a risk that newly created records will be missing from title_sort_key.
 * This should be addressed by one of these approaches (first or second is recommended, depending on the circumstances):
 *    Shut down the indexer before rebuilding title_sort_key, followed by a complete re-indexing of all pages before the scheduled indexer is restarted.
 *    Shut down the indexer before creating pages.xml and running GenerateSortNames. Restart the indexer only after all steps are complete and results reviewed.
 *    Shut down the indexer but use the most recent pages.xml created before doing so. If this approach is taken:
 *       Once this job is complete, find records in the backup that are missing from the rebuilt table and manually calculate a new value and insert.
 *       Restart the indexer only after the above step is complete.
 * 
 * User: DataAnalyst
 * Date: Oct 2024
 */
public class PopulateTitleSortKey {
   private Connection sqlCon;

   private void populateNames() {
      String insert = "INSERT INTO temp_title_sort_key (sort_key) SELECT DISTINCT sort_title FROM temp_sort_name;";

      try (PreparedStatement insertStmt = sqlCon.prepareStatement(insert)) {
         insertStmt.executeUpdate();
         commitSql();
      } catch (SQLException e) {
         rollbackSql();
         e.printStackTrace();
      }
   }

   private void calcValues(int factor) {
      int count = 1;
      Double avg = 0.0;
      int gap = 0;
      String getCount = "SELECT (SELECT (" + factor + "* (SELECT COUNT(sort_key) FROM temp_title_sort_key " + 
            "WHERE sort_key LIKE (\"user talk:%\") OR sort_key LIKE (\"image:%\") OR sort_key LIKE (\"person:%\") OR sort_key LIKE (\"person talk:%\") " + 
            "OR sort_key LIKE (\"family:%\") OR sort_key LIKE (\"family talk:%\") OR sort_key LIKE (\"mysource:%\"))) + " +
            "(SELECT COUNT(sort_key) FROM temp_title_sort_key " + 
            "WHERE sort_key NOT LIKE (\"user talk:%\") AND sort_key NOT LIKE (\"image:%\") AND sort_key NOT LIKE (\"person:%\") " + 
            "AND sort_key NOT LIKE (\"person talk:%\") AND sort_key NOT LIKE (\"family:%\") " + 
            "AND sort_key NOT LIKE (\"family talk:%\") AND sort_key NOT LIKE (\"mysource:%\"))) as count;";

      try (Statement stmt = sqlCon.createStatement()) {
         try (ResultSet rs = stmt.executeQuery(getCount)) {
            rs.next();
            count = rs.getInt("count");
         } catch (SQLException e) {
            e.printStackTrace();
         }
      } catch (SQLException e) {
         e.printStackTrace();
      }
      if (count > 0) {
         avg = ((double)Integer.MAX_VALUE - (double)Integer.MIN_VALUE) / count;
         gap = avg.intValue();
         System.out.println("count=" + count + "; normal gap=" + gap);
      }

/*      String initValue = "SET @s = " + Integer.MIN_VALUE + ";";
      try (PreparedStatement init = sqlCon.prepareStatement(initValue)) {
         init.executeUpdate();
         commitSql();
      } catch (SQLException e) {
         rollbackSql();
         e.printStackTrace();
      }
*/
      if (!setValues("WHERE sort_key < \"family talk:\"", gap, true)) {
         return;
      }
      if (!setValues("WHERE substr(sort_key,1,12) = \"family talk:\"", gap * factor, false)) {
         return;
      }
      if (!setValues("WHERE sort_key > \"family talk:\" and sort_key < \"family:\" and substr(sort_key,1,12) <> \"family talk:\"", gap, false)) {
         return;
      }
      if (!setValues("WHERE substr(sort_key,1,7) = \"family:\"", gap * factor, false)) {
         return;
      }
      if (!setValues("WHERE sort_key > \"family:\" and sort_key < \"image:\" and substr(sort_key,1,7) <> \"family:\"", gap, false)) {
         return;
      }
      if (!setValues("WHERE substr(sort_key,1,6) = \"image:\"", gap * factor, false)) {
         return;
      }
      if (!setValues("WHERE sort_key > \"image:\" and sort_key < \"mysource:\" and substr(sort_key,1,6) <> \"image:\"", gap, false)) {
         return;
      }
      if (!setValues("WHERE substr(sort_key,1,9) = \"mysource:\"", gap * factor, false)) {
         return;
      }
      if (!setValues("WHERE sort_key > \"mysource:\" and sort_key < \"person talk:\" and substr(sort_key,1,9) <> \"mysource:\"", gap, false)) {
         return;
      }
      if (!setValues("WHERE substr(sort_key,1,12) = \"person talk:\"", gap * factor, false)) {
         return;
      }
      if (!setValues("WHERE sort_key > \"person talk:\" and sort_key < \"person:\" and substr(sort_key,1,12) <> \"person talk:\"", gap, false)) {
         return;
      }
      if (!setValues("WHERE substr(sort_key,1,7) = \"person:\"", gap * factor, false)) {
         return;
      }
      if (!setValues("WHERE sort_key > \"person:\" and sort_key < \"user talk:\" and substr(sort_key,1,7) <> \"person:\"", gap, false)) {
         return;
      }
      if (!setValues("WHERE substr(sort_key,1,10) = \"user talk:\"", gap * factor, false)) {
         return;
      }
      if (!setValues("WHERE sort_key > \"user talk:\" and substr(sort_key,1,10) <> \"user talk:\"", gap, false)) {
         return;
      }
   }

   private boolean setValues (String whereClause, int gap, boolean first) {
      int start = Integer.MIN_VALUE;
      if (!first) {
         try (Statement stmt = sqlCon.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("SELECT MAX(value) as start from temp_title_sort_key;")) {
               rs.next();
               start = rs.getInt("start");
            } catch (SQLException e) {
               e.printStackTrace();
            }
         } catch (SQLException e) {
            e.printStackTrace();
         }
    
      }
/*      String initValue = "SET @s = (SELECT MAX(value) from temp_title_sort_key);";
      try (PreparedStatement init = sqlCon.prepareStatement(initValue)) {
         init.executeUpdate();
         commitSql();
      } catch (SQLException e) {
         rollbackSql();
         e.printStackTrace();
      }
*/
      String setValues = "UPDATE temp_title_sort_key t " +
            "JOIN (SELECT sort_key," + start + " + CAST((ROW_NUMBER() OVER (ORDER BY sort_key)) AS SIGNED) * " + gap + " as value " +
            "FROM temp_title_sort_key " + whereClause + ") s " +
            "ON t.sort_key = s.sort_key SET t.value = s.value;";
System.out.println("sql=" + setValues);

      try (PreparedStatement set = sqlCon.prepareStatement(setValues)) {
         set.executeUpdate();
         commitSql();
         return true;
      } catch (SQLException e) {
         rollbackSql();
         e.printStackTrace();
         return false;
      }
   }

   private void openSqlConnection(String dbHost, String userName, String password) {
      try {
         Class.forName("com.mysql.jdbc.Driver").newInstance();
         sqlCon = DriverManager.getConnection("jdbc:mysql://" + dbHost + 
                  "/wikidb?useTimezone=true&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8&user=" + 
                  userName + "&password=" + password);
         sqlCon.setAutoCommit(false);
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void purgeTable() {
      String purge = "TRUNCATE temp_title_sort_key;";
      try (Statement truncate = sqlCon.createStatement()) {
         truncate.executeUpdate(purge);
      } catch (SQLException e) {
         e.printStackTrace();
      }
   }

   private void commitSql() {
      try {
         sqlCon.commit();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void rollbackSql() {
      try {
         sqlCon.rollback();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   private void closeSqlConnection() {
      try {
         sqlCon.close();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   // Rebuild or update values in title_sort_key
   // args array: 0=flag ("all", "dedup", "calcvalues") 1=factor 2=databasehost 3=username 4=password
   public static void main(String[] args) throws IOException {

      // Initialize
      PopulateTitleSortKey self = new PopulateTitleSortKey();

      self.openSqlConnection(args[2], args[3], args[4]);

      /* De-duplicate records from temp_sort_name unless user requested only to calculate values. */
      if (!args[0].equals("calcvalues")) {
         self.purgeTable();
         self.populateNames();
      }

      /* Calculate new sort values unless user requested only to de-duplicate names. */
      if (!args[0].equals("dedup")) {
         self.calcValues(Integer.parseInt(args[1]));
      }

      self.closeSqlConnection();
      System.exit(0);
  }
}
