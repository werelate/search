package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by Dallan Quass
 * Date: May 7, 2008
 */
public final class DateStandardizeFilter extends TokenFilter
{
   public static final int MAX_BUFLEN = 256;
   private static Logger logger = Logger.getLogger("org.werelate.analysis");
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private char[] prevBuffer;

   /**
    * Construct a token stream filtering the given input.
    */
   public DateStandardizeFilter(TokenStream in)
   {
      super(in);
      prevBuffer = new char[MAX_BUFLEN];
   }

   private static boolean isDigit(char c) {
      return (c >= '0' && c <= '9');
   }

   /**
    * Return true if token is 3 or 4-digit year
    * @param buffer buffer
    * @param length length
    * @return true if is year
    */
   public static boolean isYear(char[] buffer, int length) {
      return (length == 4 &&
              buffer[0] >= '0' && buffer[0] <= '2' &&
              isDigit(buffer[1]) &&
              isDigit(buffer[2]) &&
              isDigit(buffer[3])) ||
             (length == 3 &&
              isDigit(buffer[0]) &&
              isDigit(buffer[1]) &&
              isDigit(buffer[2]));
   }

   /**
    * Return true if token is numeric day
    * @param buffer buffer
    * @param length length
    * @return true if is day
    */
   public static boolean isDay(char[] buffer, int length) {
      return (length == 1 && buffer[0] >= '1' && buffer[0] <= '9') ||
             (length == 2 &&
              ((buffer[0] == '0' && buffer[1] >= '1' && buffer[1] <= '9') ||
               (buffer[0] >= '1' && buffer[0] <= '2' && isDigit(buffer[1])) ||
               (buffer[0] == '3' && buffer[1] >= '0' && buffer[1] <= '1')));
   }

   /**
    * Return true if token is numeric month
    * @param buffer buffer
    * @param length length
    * @return true if is numeric month
    */
   public static boolean isNumMonth(char[] buffer, int length) {
      return (length == 1 && buffer[0] >= '1' && buffer[0] <= '9') ||
             (length == 2 &&
              ((buffer[0] == '0' && buffer[1] >= '1' && buffer[1] <= '9') ||
               (buffer[0] == '1' && buffer[1] >= '0' && buffer[1] <= '2')));
   }

   /**
    * Return true if token is OR (any case)
    * @param buffer token buffer
    * @param length token length
    * @return true if token is OR
    */
   public static boolean isBreak(char[] buffer, int length) {
      return length == 2 &&
              (buffer[0] == 'O' || buffer[0] == 'o') &&
              (buffer[1] == 'R' || buffer[1] == 'r');
   }

   private static final CharArrayMap<char[]> MONTHS = new CharArrayMap<char[]>(Version.LUCENE_23,12,false);
   static {
      char[] c =
      MONTHS.put("january","01".toCharArray());
      MONTHS.put("jan","01".toCharArray());
      MONTHS.put("february","02".toCharArray());
      MONTHS.put("febr","02".toCharArray());
      MONTHS.put("feb","02".toCharArray());
      MONTHS.put("march","03".toCharArray());
      MONTHS.put("mar","03".toCharArray());
      MONTHS.put("april","04".toCharArray());
      MONTHS.put("apr","04".toCharArray());
      MONTHS.put("may","05".toCharArray());
      MONTHS.put("june","06".toCharArray());
      MONTHS.put("jun","06".toCharArray());
      MONTHS.put("july","07".toCharArray());
      MONTHS.put("jul","07".toCharArray());
      MONTHS.put("august","08".toCharArray());
      MONTHS.put("aug","08".toCharArray());
      MONTHS.put("september","09".toCharArray());
      MONTHS.put("sept","09".toCharArray());
      MONTHS.put("sep","09".toCharArray());
      MONTHS.put("october","10".toCharArray());
      MONTHS.put("oct","10".toCharArray());
      MONTHS.put("november","11".toCharArray());
      MONTHS.put("nov","11".toCharArray());
      MONTHS.put("december","12".toCharArray());
      MONTHS.put("dec","12".toCharArray());
   }

   public static char[] getAlphaMonth(char[] buffer, int length) {
      return MONTHS.get(buffer, 0, length);
   }

   private static final char[] ZEROES = {'0','0','0','0'};

   private static void appendZeroFill(char[] source, char[] target, int start, int length) {
      if (source.length < length) {
         System.arraycopy(ZEROES, 0, target, start, length - source.length);
         start += length - source.length;
      }
      System.arraycopy(source, 0, target, start, source.length);
   }

   private char[] cloneTermBuffer(char[] buffer, int length) {
      char[] buf = new char[length];
      System.arraycopy(buffer, 0, buf, 0, buf.length);
      return buf;
   }

   /**
    * Returns the next input Token whose termText() is a date; standardizes the date into CCYYMMDD form
    */
   @Override
   public final boolean incrementToken() throws IOException {
      char[] year = null;
      char[] month = null;
      char[] day = null;
      char[] maybeDay = null;
      char[] alphaMonth = null;
      int prevLength = 0;

      while (input.incrementToken()) {
         char[] buffer = termAtt.buffer();
         int length = termAtt.length();
         if (isBreak(buffer, length)) {
            if (year != null) break;
         }
         else if (isYear(buffer, length)) {
            if (year == null) year = cloneTermBuffer(buffer, length);  // ignore later years
         }
         else if ((alphaMonth = getAlphaMonth(buffer, length)) != null) {
            if (month == null) month = cloneTermBuffer(alphaMonth, alphaMonth.length); // ignore later months
         }
         else if (isDay(buffer, length)) {
            if (prevLength == 0 || !isYear(prevBuffer, prevLength)) { // don't treat 1963/4 like a day/month
               if (isNumMonth(buffer, length)) {
                  if (maybeDay == null) maybeDay = cloneTermBuffer(buffer, length);
               }
               else {
                  if (day == null) day = cloneTermBuffer(buffer, length);
               }
            }
         }

         if (length < prevBuffer.length) {
            System.arraycopy(buffer, 0, prevBuffer, 0, length);
            prevLength = length;
         }
      }

      // return something
      if (year != null) {
         char[] buffer = termAtt.buffer();
         if (buffer.length < 8) {
            buffer = termAtt.resizeBuffer(8);
         }
         appendZeroFill(year, buffer, 0, 4);
         if (month != null || maybeDay != null) {
            if (month != null) {
               appendZeroFill(month, buffer, 4, 2);
            }
            else {
               appendZeroFill(maybeDay, buffer, 4, 2);
               maybeDay = null;
            }
            if (day != null || maybeDay != null) {
               if (day != null) {
                  appendZeroFill(day, buffer, 6, 2);
               }
               else {
//logger.warning("MaybeDay="+(new String(maybeDay))+": maybeDay.length="+maybeDay.length +" buffer.length="+buffer.length);                  
                  appendZeroFill(maybeDay, buffer, 6, 2);
               }
               termAtt.setLength(8);
            }
            else {
               termAtt.setLength(6);
            }
         }
         else {
            termAtt.setLength(4);
         }
         return true;
      }

      return false;
   }
}
