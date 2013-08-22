package org.werelate.analysis;

import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.common.ResourceLoader;
import org.apache.lucene.analysis.TokenStream;

/**
 * Created by Dallan Quass
 * Date: May 21, 2008
 */
public class TruncateFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

   private int length;

   public void inform(ResourceLoader loader) {
      this.length = Integer.parseInt(args.get("length"));
   }

   public TokenStream create(TokenStream input) {
      return new TruncateFilter(input, length);
   }
}
