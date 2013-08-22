package org.werelate.analysis;

import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.lucene.analysis.TokenStream;

/**
 * Created by Dallan Quass
 * Date: Apr 29, 2008
 */
public class HtmlCharacterEntitiesFilterFactory extends BaseTokenFilterFactory
{
   public TokenStream create(TokenStream tokenStream)
   {
      return new HtmlCharacterEntitiesFilter(tokenStream);
   }
}
