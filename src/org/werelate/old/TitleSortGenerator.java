package org.werelate.old;

import nu.xom.ParsingException;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import org.werelate.util.*;
import org.apache.commons.cli.*;

/**
 * Created by Dallan Quass
 * Date: Apr 28, 2008
 * !!!THIS CLASS IS DEPRECATED!!! -- replaced by GenerateSortKeys in wikidata project
 */
public class TitleSortGenerator
{
   private static Logger logger = Logger.getLogger("org.werelate.wiki");
   private static final int INTERVAL = 10;
   private static final int MAX_TX_SIZE = 10000;

   private static class TitleCount {
      int cnt;
      int max;

      private TitleCount(int cnt, int max) {
         this.cnt = cnt;
         this.max = max;
      }
   }

   private InputStream in;
   private DatabaseConnectionHelper conn;
   private Map<String, TitleCount> counts;
   private PreparedStatement psClear;
   private PreparedStatement psInsert;

   private class TitleParser implements WikiPageParser {
      public void parse(String title, String text) throws IOException, ParsingException {
         String[] namespaceTitle = Utils.splitNamespaceTitle(title);

         // prepare title for table and get index number
         String[] titleIndex = TitleSorter.splitTitleIndexNumber(namespaceTitle[1]);
         int ixNumber = 0;
         if (titleIndex[1] != null) {
            ixNumber = Integer.parseInt(titleIndex[1]);
         }

         TitleCount tc = counts.get(titleIndex[0]);
         if (tc == null) {
            tc = new TitleCount(0,0);
            counts.put(titleIndex[0], tc);
         }

         if (ixNumber > tc.max) {
            tc.max = ixNumber;
         }
         tc.cnt++;
      }
   }

   public TitleSortGenerator(InputStream in, DatabaseConnectionHelper conn) throws SQLException
   {
      this.in = in;
      this.conn = conn;
      counts = new TreeMap<String, TitleCount>();
      this.psClear = conn.preparedStatement("DELETE from title_sort");
      this.psInsert = conn.preparedStatement("INSERT IGNORE INTO title_sort VALUES (?,?)");
   }

   private void doubleTitles() {
      for (String title : counts.keySet()) {
         TitleCount tc = counts.get(title);
         if (tc.max > 0) {
            tc.max += tc.cnt;
         }
         tc.cnt += tc.cnt;
      }
   }

   private void clearTable() throws SQLException
   {
      psClear.executeUpdate();
   }

   private void writeRow(String title, int indexNumber, int sortValue) throws SQLException
   {
      if (indexNumber > 0) {
         title = TitleSorter.joinTitleIndexNumber(title, indexNumber);
      }
//      logger.info("insert title="+title+" sortValue="+Integer.toString(sortValue));
      psInsert.setString(1, title);
      psInsert.setInt(2, sortValue);
      psInsert.executeUpdate();
   }

   private void writeTable() throws SQLException
   {
      int runningTotal = 0;
      int sortValue = 0;
      int txSize = 0;

      conn.startTransaction();

      for (String title : counts.keySet()) {
         TitleCount tc = counts.get(title);

         if (runningTotal + tc.cnt > INTERVAL) {
            writeRow(title, 0, sortValue);
            if (++txSize > MAX_TX_SIZE) {
               txSize = 0;
               conn.endTransaction(true);
               System.out.print(".");
               conn.startTransaction();
            }
            sortValue++;

            for (int i = INTERVAL; i < tc.cnt; i+=INTERVAL) {
               writeRow(title, i * tc.max / tc.cnt, sortValue);
               if (++txSize > MAX_TX_SIZE) {
                  txSize = 0;
                  conn.endTransaction(true);
                  System.out.print(".");
                  conn.startTransaction();
               }
               sortValue++;
            }
            runningTotal = tc.cnt % INTERVAL; // new code - does it add a lot of new rows in the database?
         }
         else {
            runningTotal += tc.cnt;
         }
      }

      conn.endTransaction(true);
   }

   public void create() throws IOException, ParsingException, SQLException
   {
      // process XML file
      System.out.print("Reading XML file");
      WikiReader wikiReader = new WikiReader();
      wikiReader.setSkipRedirects(true);
      TitleParser titleParser = new TitleParser();
      wikiReader.addWikiPageParser(titleParser);
      wikiReader.read(in);
      in.close();
      System.out.println();

      // double titles
      doubleTitles();

      // clear out previous index entries
      clearTable();

      // write index entries
      System.out.print("Writing table");
      writeTable();
   }

   public static void main(String[] args) throws IOException, ParsingException, ParseException, IllegalAccessException, InstantiationException, ClassNotFoundException, SQLException
   {
      Options opt = new Options();
      opt.addOption("x", true, "pages.xml file");
      opt.addOption("d", true, "Database host");
      opt.addOption("u", true, "User name");
      opt.addOption("p", true, "Password");
      opt.addOption("h", false, "Print out help information");

      BasicParser parser = new BasicParser();
      CommandLine cl = parser.parse(opt, args);
      if (cl.hasOption("h") || !cl.hasOption("x") || !cl.hasOption("d") || !cl.hasOption("u") || !cl.hasOption("p"))
      {
          System.out.println("Re-generate title sort table.");
          HelpFormatter f = new HelpFormatter();
          f.printHelp("OptionsTip", opt);
          System.exit(1);
      }
      // read parameters
      InputStream in = new FileInputStream(cl.getOptionValue("x"));
      String dbUrl = "jdbc:mysql://"+cl.getOptionValue("d") + "/wikidb";
      String userName = cl.getOptionValue("u");
      String password = cl.getOptionValue("p");
      DatabaseConnectionHelper conn = new DatabaseConnectionHelper(dbUrl, userName, password);
      conn.connect();

      TitleSortGenerator ttg = new TitleSortGenerator(in, conn);
      ttg.create();
   }
}
