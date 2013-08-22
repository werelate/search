package org.werelate.search;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.AttributeSource;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.highlight.*;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocIterator;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.core.Config;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.solr.util.plugin.PluginInfoInitialized;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.werelate.util.Utils;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.logging.Logger;

/**
 * Copied from DefaultSolrHighlighter in order to add fieldSeparator
 * Date: Jun 18, 2008
 */
public class MyHighlighter extends DefaultSolrHighlighter
{
   private static Logger logger = Logger.getLogger("org.werelate.search");

   public MyHighlighter() {
      super();
   }

   public MyHighlighter(SolrCore solrCore) {
      super(solrCore);
   }

   @SuppressWarnings("unchecked")
   public NamedList<Object> doHighlighting(DocList docs, Query query, SolrQueryRequest req, String[] defaultFields) throws IOException {
      SolrParams params = req.getParams();
      if (!isHighlightingEnabled(params))
         return null;

      String fieldSeparator = params.get("hl.field.separator", "? ");
      SolrIndexSearcher searcher = req.getSearcher();
      IndexSchema schema = searcher.getSchema();
      NamedList fragments = new SimpleOrderedMap();
      String[] fieldNames = getHighlightFields(query, req, defaultFields);
      Document[] readDocs = new Document[docs.size()];
      {
         // pre-fetch documents using the Searcher's doc cache
         Set<String> fset = new HashSet<String>();
         for(String f : fieldNames) { fset.add(f); }
         // fetch unique key if one exists.
         SchemaField keyField = schema.getUniqueKeyField();
         if(null != keyField)
            fset.add(keyField.getName());
         searcher.readDocs(readDocs, docs, fset);
      }


      // Highlight each document
      DocIterator iterator = docs.iterator();
      for (int i = 0; i < docs.size(); i++) {
         int docId = iterator.nextDoc();
         Document doc = readDocs[i];
         NamedList docSummaries = new SimpleOrderedMap();
         for (String fieldName : fieldNames) {
            fieldName = fieldName.trim();
            String[] docTexts = doc.getValues(fieldName);
            if (docTexts == null) continue;

            // get highlighter, and number of fragments for this field
            org.apache.lucene.search.highlight.Highlighter highlighter = getHighlighter(query, fieldName, req);
            int numFragments = getMaxSnippets(fieldName, params);
            boolean mergeContiguousFragments = isMergeContiguousFragments(fieldName, params);

            String[] summaries = null;
            TextFragment[] frag;
            try {
               if (docTexts.length == 1) {
                  // single-valued field
                  TokenStream tstream;
                  try {
                     // attempt term vectors
                     tstream = TokenSources.getTokenStream(searcher.getReader(), docId, fieldName);
                  }
                  catch (IllegalArgumentException e) {
                     // fall back to analyzer
                     tstream = new TokenOrderingFilter(schema.getAnalyzer().tokenStream(fieldName, new StringReader(docTexts[0])), 10);
                  }
                  frag = highlighter.getBestTextFragments(tstream, docTexts[0], mergeContiguousFragments, numFragments);
               }
               else {
                  // multi-valued field
                  MultiValueTokenStream tstream;
                  tstream = new MultiValueTokenStream(fieldName, docTexts, schema.getAnalyzer(), true, fieldSeparator);
                  frag = highlighter.getBestTextFragments(tstream, tstream.asSingleValue(), false, numFragments);
               }
            } catch (InvalidTokenOffsetsException e) {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
            }

            // convert fragments back into text
            // we can include score and position information in output as snippet attributes
            if (frag.length > 0) {
               ArrayList<String> fragTexts = new ArrayList<String>();
               for (int j = 0; j < frag.length; j++) {
                  if ((frag[j] != null) && (frag[j].getScore() > 0)) {
                     fragTexts.add(frag[j].toString());
                  }
               }
               summaries = fragTexts.toArray(new String[0]);
               if (summaries.length > 0)
                  docSummaries.add(fieldName, summaries);
            }
            // no summeries made, copy text from alternate field
            if (summaries == null || summaries.length == 0) {
               String alternateField = req.getParams().getFieldParam(fieldName, HighlightParams.ALTERNATE_FIELD);
               if (alternateField != null && alternateField.length() > 0) {
                  String[] altTexts = doc.getValues(alternateField);
                  if (altTexts != null && altTexts.length > 0){
                     // WERELATE - concatenate alt texts into a single value
                     int alternateFieldLen = req.getParams().getFieldInt(fieldName, HighlightParams.ALTERNATE_FIELD_LENGTH,0);
                     int len = 0;
                     StringBuilder buf = new StringBuilder();
                     for( String altText: altTexts ){
                        if (buf.length() > 0) {
                           buf.append(fieldSeparator);
                           len += fieldSeparator.length();
                        }
                        if (alternateFieldLen > 0 && len + altText.length() > alternateFieldLen) {
                           if (len < alternateFieldLen) {
                              buf.append(altText.substring(0, alternateFieldLen - len));
                           }
                        }
                        else {
                           buf.append(altText);
                        }
                        len += altText.length();
                        if (alternateFieldLen > 0 && len >= alternateFieldLen) {
                           break;
                        }
                     }
                     summaries = new String[1];
                     summaries[0] = buf.toString();
                     docSummaries.add(fieldName, summaries);
                  }
               }
            }
         }
         String printId = schema.printableUniqueKey(doc);
         fragments.add(printId == null ? null : printId, docSummaries);
      }
      return fragments;
   }
}

/**
 * Helper class which creates a single TokenStream out of values from a
 * multi-valued field.
 */
class MultiValueTokenStream extends TokenStream {
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
   private final PositionIncrementAttribute posAtt = addAttribute(PositionIncrementAttribute.class);
   private CharTermAttribute currentTermAtt;
   private OffsetAttribute currentOffsetAtt;
   private PositionIncrementAttribute currentPosAtt;

   private String fieldName;
   private String[] values;
   private Analyzer analyzer;
   private int curIndex;                  // next index into the values array
   private int curOffset;                 // offset into concatenated string
   private TokenStream currentStream;     // tokenStream currently being iterated
   private boolean orderTokenOffsets;
   private String fieldSeparator;

   /** Constructs a TokenStream for consecutively-analyzed field values
    *
    * @param fieldName name of the field
    * @param values array of field data
    * @param analyzer analyzer instance
    */
   public MultiValueTokenStream(String fieldName, String[] values,
                                Analyzer analyzer, boolean orderTokenOffsets, String fieldSeparator) {
      this.fieldName = fieldName;
      this.values = values;
      this.analyzer = analyzer;
      curIndex = -1;
      curOffset = 0;
      currentStream = null;
      this.orderTokenOffsets=orderTokenOffsets;
      this.fieldSeparator=fieldSeparator;
      this.currentTermAtt = null;
      this.currentOffsetAtt = null;
      this.currentPosAtt = null;
   }

   /** Returns the next token in the stream, or null at EOS. */
   @Override
   public boolean incrementToken() throws IOException {
      int extra = 0;
      if(currentStream == null) {
         curIndex++;
         if(curIndex < values.length) {
            currentStream = analyzer.tokenStream(fieldName,
                    new StringReader(values[curIndex]));
            if (orderTokenOffsets) currentStream = new TokenOrderingFilter(currentStream,10);
            currentOffsetAtt = currentStream.addAttribute(OffsetAttribute.class);
            currentTermAtt = currentStream.addAttribute(CharTermAttribute.class);
            currentPosAtt = currentStream.addAttribute(PositionIncrementAttribute.class);
            // add extra space between multiple values
            if (curIndex > 0) extra = analyzer.getPositionIncrementGap(fieldName);
         } else {
            return false;
         }
      }
      if (!currentStream.incrementToken()) {
         // WERELATE - add space for field separator
         curOffset += values[curIndex].length() + fieldSeparator.length();
         currentStream = null;
         return incrementToken();
      }
      // create an modified token which is the offset into the concatenated
      // string of all values
      Utils.setTermBuf(termAtt, currentTermAtt.buffer(), currentTermAtt.length());
      offsetAtt.setOffset(currentOffsetAtt.startOffset() + curOffset, currentOffsetAtt.endOffset() + curOffset);
      posAtt.setPositionIncrement(currentPosAtt.getPositionIncrement() + extra*10);
      return true;
   }

   /**
    * Returns all values as a single String into which the Tokens index with
    * their offsets.
    */
   public String asSingleValue() {
      StringBuilder sb = new StringBuilder();
      for(String str : values) {
         // WERELATE - add field separator
         if (sb.length() > 0) sb.append(fieldSeparator);
         sb.append(str);
      }
      return sb.toString();
   }

}


/** Orders Tokens in a window first by their startOffset ascending.
 * endOffset is currently ignored.
 * This is meant to work around fickleness in the highlighter only.  It
 * can mess up token positions and should not be used for indexing or querying.
 */
final class TokenOrderingFilter extends TokenFilter {
  private final int windowSize;
  private final LinkedList<OrderedToken> queue = new LinkedList<OrderedToken>();
  private boolean done=false;
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);

  protected TokenOrderingFilter(TokenStream input, int windowSize) {
    super(input);
    this.windowSize = windowSize;
  }

  @Override
  public boolean incrementToken() throws IOException {
    while (!done && queue.size() < windowSize) {
      if (!input.incrementToken()) {
        done = true;
        break;
      }

      // reverse iterating for better efficiency since we know the
      // list is already sorted, and most token start offsets will be too.
      ListIterator<OrderedToken> iter = queue.listIterator(queue.size());
      while(iter.hasPrevious()) {
        if (offsetAtt.startOffset() >= iter.previous().startOffset) {
          // insertion will be before what next() would return (what
          // we just compared against), so move back one so the insertion
          // will be after.
          iter.next();
          break;
        }
      }
      OrderedToken ot = new OrderedToken();
      ot.state = captureState();
      ot.startOffset = offsetAtt.startOffset();
      iter.add(ot);
    }

    if (queue.isEmpty()) {
      return false;
    } else {
      restoreState(queue.removeFirst().state);
      return true;
    }
  }
}

// for TokenOrderingFilter, so it can easily sort by startOffset
class OrderedToken {
  AttributeSource.State state;
  int startOffset;
}
