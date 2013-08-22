package org.werelate.old;

import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.*;
import org.apache.solr.common.util.NamedList;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.document.Document;
import org.werelate.old.TitleSorter;
import org.werelate.util.Utils;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.Map;
import java.util.TreeMap;
import java.util.Comparator;

/**
 * Created by Dallan Quass
 * Date: Jun 4, 2008
 */
public class TitleSortComponent extends SearchComponent
{
   private static final int TITLE_SORT_EXTRA = 20;
   protected static Logger logger = Logger.getLogger("org.werelate.search");

   private boolean isTitleSort;
   private int origRows;
   private int origStart;
   private boolean isDescSort;

   public void prepare(ResponseBuilder rb) throws IOException
   {
      isTitleSort = false;
      SortSpec ss = rb.getSortSpec();
      Sort sort = ss.getSort();
      if (sort != null) {
         SortField[] sfs = sort.getSort();
         if (sfs != null && sfs.length > 0 && Utils.FLD_TITLE_SORT_VALUE.equals(sfs[0].getField())) {
            isTitleSort = true;
            isDescSort = sfs[0].getReverse();

            origRows = ss.getCount();
            origStart = ss.getOffset();
            rb.setSortSpec(new SortSpec(sort, 0, origStart+origRows+TITLE_SORT_EXTRA));
         }
      }
   }

   private static class DescComparator implements Comparator<String> {

      public int compare(String string, String string1)
      {
         return string1.compareTo(string);
      }
   }

   @SuppressWarnings("unchecked")
   public void process(ResponseBuilder rb) throws IOException
   {
      if (isTitleSort) {
         NamedList responseValues = rb.rsp.getValues();
         int pos = responseValues.indexOf("response", 0);
         if (pos >= 0) {
            DocList docs = (DocList)responseValues.getVal(pos);
            if (docs != null) {
               SolrIndexSearcher searcher = rb.req.getSearcher();
               Map<String,ScoreDoc> docMap;
               if (isDescSort) {
                  docMap = new TreeMap<String,ScoreDoc>(new DescComparator());
               }
               else {
                  docMap = new TreeMap<String,ScoreDoc>();
               }
               String prevTitleSort = "";
               int docCnt = 0;
               boolean hasScores = docs.hasScores();

               DocIterator i = docs.iterator();
               boolean broke = false;
//logger.warning("TitleSortComponent");
               while (i.hasNext()) {
                  int docNum = i.nextDoc();
                  float score;
                  if (hasScores) {
                     score = i.score();
                  }
                  else {
                     score = 0.0f;
                  }
                  String docMapKey;
                  String titleSort;
                  if (docCnt < origStart - TITLE_SORT_EXTRA) {
                     docMapKey = " "+Integer.toString(docNum); // don't bother reading these documents; they're so early that we want to sort them to the front
                     titleSort = "";
                  }
                  else {
                     Document doc = searcher.doc(docNum);
                     titleSort = doc.getFieldable(Utils.FLD_TITLE_SORT_VALUE).stringValue();
                     if (docCnt >= origStart + origRows && !titleSort.equals(prevTitleSort)) {
                        broke = true;
                        break;
                     }
                     String origTitleStored = doc.getFieldable(Utils.FLD_TITLE_STORED).stringValue();
                     String titleStored = TitleSorter.prepareTitle(origTitleStored);
                     String namespace = doc.getFieldable(Utils.FLD_NAMESPACE_STORED).stringValue();
                     // append orig title and namespace to ensure uniqueness
                     // space out namespace so shorter titles still sort above longer titles
                     docMapKey = titleStored+"   "+namespace+":"+origTitleStored;
//logger.warning("titleSort="+titleSort+" titleStored="+titleStored+" namespace="+namespace);
                  }
                  docMap.put(docMapKey, new ScoreDoc(docNum, score));
                  docCnt++;
                  prevTitleSort = titleSort;
               }
               if (docCnt > origStart + origRows && !broke) {
                  logger.warning("NEED MORE RECORDS TO SORT");
               }
               int numDocs = Math.min(docCnt, origStart+origRows);
               int[] sortedDocNums = new int[numDocs];
               float[] sortedScores;
               if (hasScores) {
                  sortedScores = new float[numDocs];
               }
               else {
                  sortedScores = null;
               }
               docCnt = 0;
               for (ScoreDoc sd : docMap.values()) {
                  sortedDocNums[docCnt] = sd.doc;
                  if (hasScores) {
                     sortedScores[docCnt] = sd.score;
                  }
                  docCnt++;
                  if (docCnt >= numDocs) {
                     break;
                  }
               }
               int length = Math.max(0, numDocs - origStart);
               DocSlice sortedDocs = new DocSlice(origStart, length, sortedDocNums, sortedScores, docs.matches(), docs.maxScore());
               responseValues.setVal(pos, sortedDocs);
            }
         }
      }
   }

   /////////////////////////////////////////////
   ///  SolrInfoMBean
   ////////////////////////////////////////////

   public String getDescription() {
     return "titlesort";
   }

   public String getVersion() {
     return "$Revision$";
   }

   public String getSourceId() {
     return "$Id: QueryComponent.java 649066 2008-04-17 12:37:38Z gsingers $";
   }

   public String getSource() {
     return "$URL: http://svn.apache.org/repos/asf/lucene/solr/trunk/src/java/org/apache/solr/handler/component/QueryComponent.java $";
   }

}
