package org.werelate.analysis;

import org.apache.solr.analysis.BaseTokenizerFactory;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.solr.common.ResourceLoader;

import java.io.Reader;

/**
 * Created by Dallan Quass
 * Date: May 8, 2008
 */
public class NameTokenizerFactory extends BaseTokenizerFactory  implements ResourceLoaderAware {
   private boolean isSurname;

   public void inform(ResourceLoader loader) {
      isSurname = args.get("surname").equals("true");
   }

  public NameTokenizer create(Reader input) {
    return new NameTokenizer(input, isSurname);
  }
}
