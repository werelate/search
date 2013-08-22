package org.werelate.search;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.common.util.NamedList;
import org.werelate.util.Utils;

import java.util.*;
import java.io.StringReader;
import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 22, 2008
 */
public class PlaceRewriteComponent extends BaseRewriteComponent
{
   public static final String IS_SOURCE_NAMESPACE = "p_is_source_namespace";
   public static final String RESPONSE_MATCHES = "p_reponse_matches";

   public PlaceRewriteComponent() {
   }

   protected void pre(BooleanQuery in, ResponseBuilder rb) {
      Map<Object,Object> context = rb.req.getContext();
      boolean isSourceNamespace = hasNamespaceFilter(rb.getFilters(), Utils.NS_SOURCE_TEXT);
      context.put(IS_SOURCE_NAMESPACE, isSourceNamespace);
      context.put(RESPONSE_MATCHES, new HashMap<String,List<String>>());
   }

   public static String getAnalyzedPlaceText(String text, Analyzer analyzer) {
      String result = null;
      try
      {
         // reverses the levels
         TokenStream ts = analyzer.reusableTokenStream(Utils.FLD_PLACE_TITLE, new StringReader(text));
         CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
         if (ts.incrementToken()) {
            result = new String(termAttr.buffer(), 0, termAttr.length());
         }
         ts.end();
         ts.close();
      } catch (IOException e)
      {
         logger.warning("Error analyzing: " + text + " : " + e.getMessage());
      }
      return result;
   }

   private Term getTerm(String fieldName, String text, Analyzer analyzer) {
      String fieldValue = getAnalyzedPlaceText(text, analyzer);
      if (fieldValue != null) {
         return new Term(fieldName, fieldValue);
      }
      return null;
   }

   protected void rewrite(TermQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("TQ q="+q.toString()+" fld="+q.getTerm().field()+" value="+q.getTerm().text());
      String fieldName = q.getTerm().field();
      if (fieldName.endsWith("Place")) {
         Analyzer analyzer = rb.req.getSchema().getQueryAnalyzer();
         boolean isSourceNamespace = (Boolean)rb.req.getContext().get(IS_SOURCE_NAMESPACE);
         boolean includeSubplaces = !Utils.isEmpty(rb.req.getParams().get("sub"));
         PlaceSearcher placeSearcher = new PlaceSearcher(rb.req.getSearcher(), rb.req.getSchema().getQueryAnalyzer());

         String text = q.getTerm().text();
         String[] matches = placeSearcher.getMatches(text);
         DisjunctionMaxQuery resultQ = null;
         if ((matches == null || matches.length == 0) && text.indexOf(',') < 0) {
            String placePrefix = getAnalyzedPlaceText(text, analyzer);
            if (placePrefix != null) {
               PrefixQuery pq = new PrefixQuery(new Term(fieldName, placePrefix+","));
               out.add(pq, occur);
            }
         }
         else if (matches != null) {
            if (matches.length > 1) {
               resultQ = new DisjunctionMaxQuery(0.0f);
               resultQ.setBoost(q.getBoost());
            }
            for (String match : matches) {
               // if we're not including subplaces, add no-subplace to term
               if (isSourceNamespace && !includeSubplaces) {
                  match = Utils.NO_SUBPLACE+","+match;
               }
               Term t = getTerm(fieldName, match, analyzer);
               if (t != null) {
                  TermQuery tq = new TermQuery(t);
                  tq.setBoost(q.getBoost());
                  if (resultQ != null) {
                     resultQ.add(tq);
                  }
                  else {
                     out.add(tq, occur);
                  }
               }
            }
         }
         if (resultQ != null) {
            out.add(resultQ, occur);
         }

         if (matches != null && matches.length > 1) {
            @SuppressWarnings({"unchecked"})
            Map<String,List<String>> responseMatches = (Map<String,List<String>>)rb.req.getContext().get(RESPONSE_MATCHES);
            responseMatches.put(fieldName, Arrays.asList(matches));
         }
      }
      else {
         out.add(q, occur);
      }
   }

   // if querying multiple places, say so back to the user
   @SuppressWarnings("unchecked")
   public void process(ResponseBuilder rb) throws IOException
   {
      super.process(rb);

      Map<String,List<String>> responseMatches = (Map<String,List<String>>)rb.req.getContext().get(RESPONSE_MATCHES);
      if (responseMatches.keySet().size() > 0) {
         NamedList  nl = new NamedList();
         for (String place : responseMatches.keySet()) {
            nl.add(place, responseMatches.get(place));
         }
         rb.rsp.add("placeRewrite", nl);
      }
   }

   /////////////////////////////////////////////
   ///  SolrInfoMBean
   ////////////////////////////////////////////

   public String getDescription() {
     return "PlaceRewriteComponent";
   }

   public String getVersion() {
     return "1.1";
   }

   public String getSourceId() {
     return "PlaceRewriteComponent";
   }

   public String getSource() {
     return "PlaceRewriteComponent";
   }

}
