package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.werelate.util.Utils;

import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 28, 2008
 */
public class PlaceReverseFilter extends TokenFilter
{
   public static final int MAX_BUFLEN = 1024;
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private char[] buffer;

   public PlaceReverseFilter(TokenStream in) {
      super(in);
      buffer = new char[MAX_BUFLEN];
   }

   public static int reverseBuffer(char[] src, int srcLen, char[] target) {
      // work your way backward from the end to the beginning
      int targetLen = 0;
      int srcEnd = srcLen;
      while (srcEnd > 0) {
         int srcBegin = srcEnd - 1;
         int charBegin = -1;
         int charEnd = -1;
         while (srcBegin >= 0 && src[srcBegin] != ',') {
            if (src[srcBegin] != ' ') {
               charBegin = srcBegin;
               if (charEnd == -1) {
                  charEnd = srcBegin+1;
               }
            }
            srcBegin--;
         }
         if (srcBegin < 0 && charBegin == 0 && charEnd == srcLen) {
            return 0; // no commas, and no trimming necessary
         }
         if (charBegin >= 0) {
            // copy from charBegin up to charEnd into target
            int len = charEnd - charBegin;
            if (targetLen + 1 + len > target.length) {
               break; // too long
            }
            if (targetLen > 0) {
               // append a , to target
               target[targetLen++] = ',';
            }
            System.arraycopy(src, charBegin, target, targetLen, len);
            targetLen += len;
         }
         srcEnd = srcBegin;
      }
      return targetLen;
   }

   @Override
   public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
         int resultLen = reverseBuffer(termAtt.buffer(), termAtt.length(), buffer);
         if (resultLen > 0) {
            Utils.setTermBuf(termAtt, buffer, resultLen);
         }
         return true;
     }
     return false;
   }
}
