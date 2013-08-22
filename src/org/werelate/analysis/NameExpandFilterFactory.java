package org.werelate.analysis;

import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.common.ResourceLoader;
import org.apache.lucene.analysis.TokenStream;

/**
 * Created by Dallan Quass
 * Date: May 7, 2008
 */
public class NameExpandFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

   private boolean isSurname;

   public void inform(ResourceLoader loader) {
      isSurname = args.get("surname").equals("true");
   }

   public TokenStream create(TokenStream input) {
      return new NameExpandFilter(input, isSurname);
   }
}
