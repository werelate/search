package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.werelate.util.Utils;

import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 21, 2008
 */
public class FirstLetterFilter extends TokenFilter
{
   private static final char[] WXYZ = "WXYZ".toCharArray();
   private static final char[] OTHER = "other".toCharArray();
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

   public FirstLetterFilter(TokenStream in) {
      super(in);
   }

   @Override
   public final boolean incrementToken() throws IOException {
      if (input.incrementToken() && termAtt.length() > 0) {
         char[] termBuf = termAtt.buffer();
         char firstChar = termBuf[0];
         if (firstChar >= 'A' && firstChar < 'W') {
            termAtt.setLength(1);
         }
         else if (firstChar >= 'a' && firstChar < 'w') {
            termBuf[0] = Character.toUpperCase(firstChar);
            termAtt.setLength(1);
         }
         else if ((firstChar >= 'W' && firstChar <= 'Z') || (firstChar >= 'w' && firstChar <= 'z')) {
            Utils.setTermBuf(termAtt, WXYZ, WXYZ.length);
         }
         else {
            Utils.setTermBuf(termAtt, OTHER, OTHER.length);
         }
         return true;
      }
      return false;
   }
}
