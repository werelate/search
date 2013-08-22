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
public class TruncateFilter extends TokenFilter
{
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   int length;

   public TruncateFilter(TokenStream in, int length) {
      super(in);
      this.length = length;
   }

   @Override
   public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
         if (termAtt.length() > length) {
            termAtt.setLength(length);
         }
         return true;
      }
      return false;
   }
}
