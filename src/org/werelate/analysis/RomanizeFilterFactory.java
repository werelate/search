package org.werelate.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.analysis.BaseTokenFilterFactory;

/**
 * Created by Dallan Quass
 * Date: Apr 29, 2008
 */
public class RomanizeFilterFactory extends BaseTokenFilterFactory
{
   public TokenStream create(TokenStream tokenStream)
   {
      return new RomanizeFilter(tokenStream);
   }
}
