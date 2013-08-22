package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.werelate.util.Utils;

import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 8, 2008
 */
public class UnabbrevFilter extends TokenFilter
{
   private CharArrayMap<char[]> abbrevs;
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private boolean processFirstToken;
   private boolean isFirstToken;

   /**
    * Construct a token stream filtering the given input.
    */
   public UnabbrevFilter(TokenStream in, CharArrayMap<char[]> abbrevs, boolean processFirstToken)
   {
      super(in);
      this.abbrevs = abbrevs;
      this.processFirstToken = processFirstToken;
      this.isFirstToken = true;
   }

   /**
    * Expands the place text
    */
   @Override
   public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
         if (processFirstToken || !isFirstToken) {
            char[] unabbrev = abbrevs.get(termAtt.buffer(), 0, termAtt.length());
            if (unabbrev != null) {
               Utils.setTermBuf(termAtt, unabbrev, unabbrev.length);
            }
         }
         isFirstToken = false;
         return true;
     }
     isFirstToken = true;
     return false;
   }

   @Override
   public void reset() throws IOException {
      super.reset();
      isFirstToken = true;
   }
}
