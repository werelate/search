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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.*;
import java.sql.Connection;
import java.util.HashMap;

import nu.xom.*;

import org.werelate.util.Utils;
import org.werelate.util.WikiPageParser;
import org.werelate.util.WikiReader;
import org.werelate.wiki.PersonPageIndexer;
import org.werelate.wiki.PersonTalkPageIndexer;
import org.werelate.wiki.FamilyPageIndexer;
import org.werelate.wiki.FamilyTalkPageIndexer;
import org.werelate.wiki.SourcePageIndexer;

/**
 * This class inserts a row into table temp_sort_name for each Person and Family page.
 * User: DataAnalyst
 * Date: Oct 2024
 */
public class GenerateSortNames implements WikiPageParser {
   private Connection sqlCon;
   private static int rows=0;
   private static String[] rowValue = new String[1000];
   private String pageSortName = null;
   private Builder builder;
   
   public void parse(String fullTitle, String text) throws IOException, ParsingException {
      this.builder = new Builder();
      Document xml = null;
      int ns = 0;

      String[] namespaceTitle = Utils.splitNamespaceTitle(fullTitle); // return namespace text in field[0], title in field[1]
      String pageTitle = namespaceTitle[1];
      String namespace = namespaceTitle[0];
      String tagName = namespace.toLowerCase();
      if (!namespace.equals("")) {
         ns = Utils.NAMESPACE_MAP.get(namespace);
      }

      if (Utils.isEmpty(text) || text.length() < 9 || !(text.substring(0,9).toUpperCase().equals("#REDIRECT"))) {  // Ignore redirected pages
         String titleSort = null;
         String fullname = null;
         String surnameIndex = null;
         if (ns==108 || ns==110) {
            String[] split = Utils.splitStructuredWikiText(tagName, text);
            xml = Utils.parseText(builder, split[0], true);
            if (ns==108) {
               titleSort = getPersonTitleSort(fullTitle, xml);
               Nodes nodes = xml.query("person/name");
               if (nodes.size() > 0) {
                  fullname = PersonPageIndexer.getReversedFullname((Element)nodes.get(0));
               }
               else {
                  fullname = PersonPageIndexer.getReversedTitle(pageTitle);
               }
               surnameIndex = PersonPageIndexer.getSurnameIndex(pageSortName);
            }
            else {
               pageSortName = FamilyPageIndexer.getReversedFullname(pageTitle, xml);
               titleSort = "Family:" + pageSortName;
               fullname = pageSortName;
               surnameIndex = PersonPageIndexer.getSurnameIndex(pageSortName);
            }
         }
         if (ns==109) {
            pageSortName = PersonPageIndexer.getReversedTitle(pageTitle);
            titleSort = "Person talk:" + pageSortName;
            fullname = pageSortName;
            surnameIndex = PersonPageIndexer.getSurnameIndex(pageSortName);
         }
         if (ns==111) {
            pageSortName = FamilyPageIndexer.getReversedTitle(pageTitle, null, null);
            titleSort = "Family talk:" + pageSortName;
            fullname = pageSortName;
            surnameIndex = PersonPageIndexer.getSurnameIndex(pageSortName);
         }
         if (ns < 108 || ns > 111) {
            titleSort = fullTitle;
            fullname = "";
         }

         // Finalize sort key (romanize, lowercase, trim, max 80 characters as done in TitleSorter)
         titleSort = Utils.romanize(titleSort).toLowerCase().trim();
         if (titleSort.length() > 80) {
            titleSort = titleSort.substring(0, 80);
         }

         // Ensure the strings are all ASCII characters for writing to the database (handles characters that could not be romanized)
         pageTitle = toASCII(Utils.romanize(pageTitle));
         titleSort = toASCII(titleSort);
         fullname = toASCII(Utils.romanize(fullname));
   
         // Prepare line for writing to the database
         rowValue[rows++] = " (" + ns + ",\"" + pageTitle.replace("\\","\\\\").replace("\"","\\\"") + "\",\"" + 
                  titleSort.replace("\\","\\\\").replace("\"","\\\"") + "\",\"" + fullname.replace("\\","\\\\").replace("\"","\\\"") + 
                  "\",\"" + surnameIndex + "\")";
         if (rows==500) {
            insertRows();
         }
      }
   }

   private static String toASCII(String s) {
      if (s == null) {
         return "";
      }

      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < s.length(); i++) {
         char c = s.charAt(i);
         if ((int)c > 127) {
            buf.append("?");
         }
         else {
            buf.append(c);
         }
      }
      return buf.toString();
   }

   private String getPersonTitleSort(String fullTitle, Document xml) {
      if (xml != null) {
         Nodes nodes  = xml.query("person/name");
         if (nodes.size() > 0) {
            Element name = (Element)nodes.get(0);
            if (!Utils.isEmpty(PersonPageIndexer.getNameAttr(name, "surname")) || !Utils.isEmpty(PersonPageIndexer.getNameAttr(name, "given"))) {
               pageSortName = PersonPageIndexer.getReversedSortName(name);
               return "Person:" + pageSortName;
            }
         }
      }
      pageSortName = PersonPageIndexer.getReversedTitle(fullTitle.substring(7));
      return "Person:" + pageSortName;
   }

   private void insertRows() {
      String sql = "INSERT INTO temp_sort_name (page_namespace, title_stored, sort_title, display_name, surname_index) VALUES";
      for (int i=0; i<rows; i++) {      
         sql += rowValue[i];
         if (i<(rows-1)) {
            sql += ", ";
         }
         else {
            sql += ";";
         }
      }
      try (PreparedStatement stmt = sqlCon.prepareStatement(sql)) {
         stmt.executeUpdate();
         commitSql();
      } 
      catch (SQLException e) {
         rollbackSql();
         e.printStackTrace();
      }
         
      rows = 0; 
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
      String purge = "TRUNCATE temp_sort_name;";
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

   // Test indexer code for sort title and sort display name against page.xml
   // args array: 0=pages.xml 1=databasehost 2=username 3=password
   public static void main(String[] args) throws IOException, ParsingException {

      // Initialize
      GenerateSortNames self = new GenerateSortNames();

      self.openSqlConnection(args[1], args[2], args[3]);
      self.purgeTable();
      WikiReader wikiReader = new WikiReader();
      wikiReader.setSkipRedirects(true);
      wikiReader.addWikiPageParser(self);

      InputStream in = new FileInputStream(args[0]);
      try { 
         wikiReader.read(in);
      } catch (ParsingException e) {
         e.printStackTrace();
      } finally {
         if (rows > 0) {
            self.insertRows(); // Last set of rows
         }
         in.close();
      }
      self.closeSqlConnection();
      System.exit(0);
  }
}
