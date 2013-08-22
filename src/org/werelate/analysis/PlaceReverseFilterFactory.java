package org.werelate.analysis;

import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Created by Dallan Quass
 * Date: May 28, 2008
 */
public class PlaceReverseFilterFactory extends BaseTokenFilterFactory
{
   public TokenStream create(TokenStream tokenStream)
   {
      return new PlaceReverseFilter(tokenStream);
   }
}
