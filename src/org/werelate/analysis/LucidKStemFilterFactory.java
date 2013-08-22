package org.werelate.analysis;

import com.lucidimagination.luceneworks.analysis.LucidKStemFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.analysis.BaseTokenFilterFactory;

/**
 * Created by Dallan Quass
 */
public class LucidKStemFilterFactory extends BaseTokenFilterFactory {
   public TokenStream create(TokenStream input) {
      return new LucidKStemFilter(input);
   }
}
