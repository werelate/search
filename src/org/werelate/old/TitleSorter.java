package org.werelate.old;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.text.NumberFormat;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.ResultSet;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

/**
 * Created by Dallan Quass
 * !!!DEPRECATED!!!
 */
public class TitleSorter
{
   private static final Pattern INDEX_NUMBER_PATTERN = Pattern.compile("(.*)\\((\\d+?)\\)$");
   private static final int MAX_LENGTH = 80;
   private static final int MIN_IX_DIGITS = 6;
   private static final int MAX_INT_DIGITS = 10;

   private NumberFormat nf;
   private PreparedStatement psLookup;

   // remove html character entities (unnecessary), romanize (yes), lowercase (yes), trim (yes)
   private static String cleanTitle(String title) {
      return Utils.romanize(title).toLowerCase().trim();
   }

   public static String[] splitTitleIndexNumber(String title) {
      String[] fields = new String[2];
      Matcher m = INDEX_NUMBER_PATTERN.matcher(title);
      if (m.lookingAt()) {
         fields[0] = cleanTitle(m.group(1));
         fields[1] = m.group(2);
      }
      else {
         fields[0] = cleanTitle(title);
         fields[1] = null;
      }

      if (fields[0].length() > MAX_LENGTH) {
         fields[0] = fields[0].substring(0, MAX_LENGTH);
      }
      return fields;
   }

   public static String joinTitleIndexNumber(String title, int indexNumber) {
      if (indexNumber > 0) {
         NumberFormat nf = NumberFormat.getIntegerInstance();
         nf.setMinimumIntegerDigits(MIN_IX_DIGITS);
         nf.setGroupingUsed(false);
         // add an extra space before (ddd) so that X (ddd) sorts before X () (ddd)
         return title + "  (" + nf.format(indexNumber) + ")";
      }
      else {
         return title;
      }
   }

   public static String prepareTitle(String title) {
      String[] fields = TitleSorter.splitTitleIndexNumber(title);
      return TitleSorter.joinTitleIndexNumber(fields[0], fields[1] != null ? Integer.parseInt(fields[1]) : 0);
   }

   public TitleSorter(DatabaseConnectionHelper conn) throws SQLException
   {
      this.nf = NumberFormat.getIntegerInstance();
      nf.setMinimumIntegerDigits(MAX_INT_DIGITS);
      nf.setGroupingUsed(false);
      this.psLookup = conn.preparedStatement("SELECT ts_sort FROM title_sort WHERE ts_title >= ? ORDER BY ts_title LIMIT 1");
   }

   public String sort(String title) throws SQLException
   {
      int sortKey;
      title = TitleSorter.prepareTitle(title);

      // look up in database
      psLookup.setString(1, title);
      ResultSet rs = psLookup.executeQuery();
      if (rs.next()) {
         sortKey = rs.getInt(1);
      }
      else {
         sortKey = Integer.MAX_VALUE;
      }
      rs.close();

      // return sort value
      return nf.format(sortKey);
   }

}
