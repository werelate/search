package org.werelate.analysis;

import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Created by Dallan Quass
 * Date: May 7, 2008
 */
public class DateStandardizeFilterFactory extends BaseTokenFilterFactory
{
   public TokenStream create(TokenStream tokenStream)
   {
      return new DateStandardizeFilter(tokenStream);
   }
}
