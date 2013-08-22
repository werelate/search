package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.werelate.util.Utils;

import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: Apr 29, 2008
 */
public class RomanizeFilter extends TokenFilter
{
   private static final int MAX_TOKEN_LENGTH = 1024;
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private char[] buf;

   public RomanizeFilter(TokenStream in) {
      super(in);
      buf = new char[MAX_TOKEN_LENGTH*2];
   }

   public final boolean incrementToken() throws IOException {
      if (input.incrementToken() && termAtt.length() <= MAX_TOKEN_LENGTH) {
         int resultLen = Utils.romanize(termAtt.buffer(), termAtt.length(), buf);
         if (resultLen > 0) {
            Utils.setTermBuf(termAtt, buf, resultLen);
         }
         return true;
      }
      return false;
   }
}
