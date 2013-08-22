package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.folg.names.search.Searcher;
import org.werelate.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Dallan Quass
 * Date: May 7, 2008
 */
public class NameExpandFilter extends TokenFilter
{
   public static final char DMP_PREFIX_CHAR = '_';
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
   private Searcher searcher;
   private List<String> tokens = null;
   private int tokenPosition = 0;

   /**
    * Construct a token stream filtering the given input.
    */
   public NameExpandFilter(TokenStream in, boolean isSurname) {
      super(in);
      searcher = (isSurname ? Searcher.getSurnameInstance() : Searcher.getGivennameInstance());
   }

   /**
    * Expands the name text
    */
   @Override
   public final boolean incrementToken() throws IOException {
      if (tokens != null && tokenPosition < tokens.size()) {
         String token = tokens.get(tokenPosition);
         tokenPosition++;
         char[] buf = termAtt.buffer();
         if (buf.length < token.length()+1) {
            buf = termAtt.resizeBuffer(token.length()+1);
         }
         System.arraycopy(token.toCharArray(), 0, buf, 0, token.length());
         termAtt.setLength(token.length());
         posAtt.setPositionIncrement(0);
         return true;
      }
      else if (input.incrementToken()) {
         char[] buffer = termAtt.buffer();
         int length = termAtt.length();
         String name = new String(buffer, 0, length);
         tokens = new ArrayList<String>(searcher.getAdditionalIndexTokens(name));
         tokenPosition = 0;
         return true;
      }
      return false;
   }

   @Override
   public void reset() throws IOException {
      super.reset();
   }
}
