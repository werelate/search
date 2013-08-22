package org.werelate.analysis;

import org.apache.lucene.util.Version;
import org.apache.solr.analysis.BaseTokenFilterFactory;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.solr.common.ResourceLoader;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;

import java.io.IOException;
import java.util.List;

/**
 * Created by Dallan Quass
 * Date: May 8, 2008
 */
public class UnabbrevFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {

   private CharArrayMap<char[]> abbrevs = null;
   private boolean unabbrevFirstToken;

   public void inform(ResourceLoader loader) {
      unabbrevFirstToken = "true".equals(args.get("unabbrevFirstToken"));
      boolean ignoreCase = "true".equals(args.get("ignoreCase"));
      try
      {
         List<String> lines = loader.getLines(args.get("abbrevs"));
         abbrevs = new CharArrayMap<char[]>(Version.LUCENE_23,lines.size(),ignoreCase);
         for (String line : lines) {
            String[] words = line.split(",");
            String unabbrev = words[0].trim();
            for (int i = 1; i < words.length; i++) {
               abbrevs.put(words[i].trim(), unabbrev.toCharArray());
            }
         }
      } catch (IOException e)
      {
         throw new RuntimeException("Error loading abbrevs file: " + e);
      }
   }

   public TokenStream create(TokenStream input) {
      return new UnabbrevFilter(input, abbrevs, unabbrevFirstToken);
   }
}
