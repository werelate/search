package org.werelate.analysis;

import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Created by Dallan Quass
 * Date: May 21, 2008
 */
public class FirstLetterFilterFactory extends BaseTokenFilterFactory
{
   public TokenStream create(TokenStream tokenStream)
   {
      return new FirstLetterFilter(tokenStream);
   }
}
