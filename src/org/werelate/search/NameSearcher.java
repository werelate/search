package org.werelate.search;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocIterator;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.werelate.util.Utils;
import org.werelate.analysis.NameExpandFilter;

import java.io.StringReader;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by Dallan Quass
 * Date: May 30, 2008
 * NO LONGER USED!!!
 */
public class NameSearcher
{
   private static Logger logger = Logger.getLogger("org.werelate.search");

   private static final String SURNAME_CACHE_NAME = "surname";
   private static final String GIVENNAME_CACHE_NAME = "givenname";
   private static final int MAX_RELATED_NAMES = 65;

   private final SolrIndexSearcher searcher;
   private final Analyzer analyzer;
   // TODO someday change this to a solr generic cache?
//   private static JCS surnameCache;
//   private static JCS givennameCache;

//   private static synchronized void ensureCache()
//   {
//      if (surnameCache == null)
//      {
//         try
//         {
//            surnameCache = JCS.getInstance(SURNAME_CACHE_NAME);
//            givennameCache = JCS.getInstance(GIVENNAME_CACHE_NAME);
//         } catch (CacheException e)
//         {
//            throw new RuntimeException("Couldn't instantiate cache: " + e.getMessage());
//         }
//      }
//   }

   public NameSearcher(SolrIndexSearcher searcher, Analyzer analyzer) {
      this.searcher = searcher;
      this.analyzer = analyzer;
//      ensureCache();
   }

   private String[] lookupRelatedNames(boolean isSurname, String name, String dmpName)
   {
      String[] relatedNames = null;

      try {
         String fieldName = isSurname ? Utils.FLD_SURNAME_TITLE : Utils.FLD_GIVENNAME_TITLE;
         int docNum = searcher.getFirstMatch(new Term(fieldName, name));
         if (docNum >= 0) {
            Document doc = searcher.doc(docNum);
            relatedNames = doc.getValues(Utils.FLD_RELATED_NAME);
         }
         else { // didn't find a page for this name; get all names with same DMP code
            DocSet ds = searcher.getDocSet(new TermQuery(new Term(fieldName, NameExpandFilter.DMP_PREFIX_CHAR+dmpName)));
            if (ds.size() > 0) {
               relatedNames = new String[ds.size()];
               DocIterator i = ds.iterator();
               int j = 0;
               while (i.hasNext()) {
                  Document doc = searcher.doc(i.nextDoc());
                  relatedNames[j++] = doc.getFieldable(Utils.FLD_TITLE_STORED).stringValue();
               }
            }
         }
         if (relatedNames != null && relatedNames.length > MAX_RELATED_NAMES) {
            String[] temp = relatedNames;
            relatedNames = new String[MAX_RELATED_NAMES];
            System.arraycopy(temp, 0, relatedNames, 0, MAX_RELATED_NAMES);
         }
         if (relatedNames != null) {
            // run related names through analyzer
            Token startToken = new Token();
            for (int i = 0; i < relatedNames.length; i++) {
               TokenStream ts = analyzer.reusableTokenStream(fieldName, new StringReader(relatedNames[i]));
               CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
               if (ts.incrementToken()) {
                  relatedNames[i] = new String(termAttr.buffer(), 0, termAttr.length());
               }
               ts.end();
               ts.close();
            }
            if (relatedNames.length > 30) {
               logger.warning("Name with over 30 related names: " + name);
            }
         }
      }
      catch (IOException e) {
         logger.severe("Error querying/analyzing related names for: " + name + " : " + e.toString());
      }

      if (relatedNames == null) {
         relatedNames = new String[0];
      }

      return relatedNames;
   }

   private String getCacheKey(String s) {
      return s.toLowerCase();
   }

   public String[] getRelatedNames(boolean isSurname, String name, String dmpName) {
      String[] relatedNames = null;

//      String cacheKey = getCacheKey(name);
//      if (isSurname) {
//         relatedNames = (String[]) surnameCache.get(cacheKey);
//      }
//      else {
//         relatedNames = (String[]) givennameCache.get(cacheKey);
//      }
//      if (relatedNames == null) {
//         relatedNames = lookupRelatedNames(isSurname, name, dmpName);
//
//         try
//         {
//            if (isSurname) {
//               surnameCache.put(cacheKey, relatedNames);
//            }
//            else {
//               givennameCache.put(cacheKey, relatedNames);
//            }
//         } catch (CacheException e)
//         {
//            logger.warning("Couldn't add to surname/givennameCache: " + e.getMessage());
//         }
//      }

      return relatedNames;
   }
}
