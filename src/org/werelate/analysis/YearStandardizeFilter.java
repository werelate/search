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
public final class YearStandardizeFilter extends TokenFilter {
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

  /**
   * Construct a token stream filtering the given input.
   */
  public YearStandardizeFilter(TokenStream in)
  {
     super(in);
  }

  /**
   * Returns the next input Token whose termText() is a 3 or 4-digit year
   */
   /**
    * Expands the place text
    */
   @Override
   public final boolean incrementToken() throws IOException {
      // return the first year found
      while (input.incrementToken()) {
         // TODO index os/ns dates under both years + handle ranges -- we need the date standardizer from gedcom upload
         if (DateStandardizeFilter.isYear(termAtt.buffer(), termAtt.length())) {
            return true;
         }
      }
      return false;
   }
}