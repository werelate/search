package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.werelate.util.Utils;

import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 7, 2008
 */
public class PlaceExpandFilter extends TokenFilter
{
//   public static final char[] UNITED_STATES = {'u','n','i','t','e','d',' ','s','t','a','t','e','s'};
//   private static final char[] UNKNOWN = {'u','n','k','n','o','w','n'};
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private int bufPos;
   private char[] buffer;

   /**
    * Construct a token stream filtering the given input.
    */
   public PlaceExpandFilter(TokenStream in)
   {
      super(in);
      buffer = new char[PlaceReverseFilter.MAX_BUFLEN];
      bufPos = 0;
   }

   public static int getNextPlacePosition(char[] buffer, int bufPos) {
      bufPos--;

      while (bufPos >= 0 && buffer[bufPos] != ',') {
         bufPos--;
      }

      if (bufPos > 0) { // && (includeUnitedStates || !isEqual(UNITED_STATES, buffer, bufPos))) {
         return bufPos;
      }
      return -1;
   }

   public static int getPlaceLevel(char[] buffer, int bufLen) {
      int levels = 1;
      for (int i = 0; i < bufLen; i++) {
         if (buffer[i] == ',') levels++;
      }
      return levels;
   }

   /**
    * Expands the place text
    */
   @Override
   public final boolean incrementToken() throws IOException {
      if (bufPos > 0) {
         bufPos = getNextPlacePosition(buffer, bufPos);
         if (bufPos > 0) {
            // copy buffer into result and return
            Utils.setTermBuf(termAtt, buffer, bufPos);
            return true;
         }
      }

      if (input.incrementToken()) {
         bufPos = termAtt.length();
         if (buffer.length >= bufPos) {
            System.arraycopy(termAtt.buffer(), 0, buffer, 0, bufPos);
         }
         return true;
      }
      return false;
   }

   @Override
   public void reset() throws IOException {
      super.reset();
      bufPos = 0;
   }
}
