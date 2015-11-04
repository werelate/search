package org.werelate.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.ConstantScorePrefixQuery;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.werelate.analysis.PlaceReverseFilter;
import org.werelate.util.Utils;
import org.werelate.analysis.PlaceExpandFilter;

import java.util.*;

/**
 * Created by Dallan Quass
 * Date: May 22, 2008
 */
public class BoostRewriteComponent extends BaseRewriteComponent implements NamedListInitializedPlugin
{
   public static final float WEIGHT_SURNAME = 1.65f;
   public static final float WEIGHT_GIVEN_NAME = 1.3f;
   public static final float WEIGHT_YEAR = 0.4f;
   public static final float WEIGHT_KEYWORDS = 0.5f;

   public static final float BOOST_DATE_MONTH = 0.6f;
   public static final float BOOST_DATE_YEAR = 0.4f;

   public static final float BOOST_TITLE = 1.0f;

   public static final float BOOST_NAME_RELATED = 0.6f;
   public static final float BOOST_NAME_DMP = 0.5f;
   public static final float BOOST_NAME_INITIAL = 0.2f;
   public static final float BOOST_NAME_MARR = 0.2f;

   public static final float BOOST_TITLE_WORD = 0.1f;

   public static final float[] BOOST_PLACE = {0.2f, 0.5f, 0.8f, 1.0f};
   public static final float BOOST_PLACE_NOSUB = 1.0f;

   public static final float MIN_FUZZY_SIMILARITY = 0.65f;

   public static final String IS_SOURCE_NAMESPACE = "b_is_source_namespace";
   public static final String TITLE_WORDS = "b_title_words";


   public BoostRewriteComponent() {
      super();
   }

//   public void inform(SolrCore core)
//   {
//      commonSurnames = WordlistManager.getInstance().getWordlist("surnames.txt", core.getResourceLoader());
//      commonGivennames = WordlistManager.getInstance().getWordlist("givennames.txt", core.getResourceLoader());
//   }

   protected void pre(BooleanQuery in, ResponseBuilder rb) {
      Map<Object,Object> context = rb.req.getContext();
      boolean isSourceNamespace = hasNamespaceFilter(rb.getFilters(), Utils.NS_SOURCE_TEXT);
      context.put(IS_SOURCE_NAMESPACE, isSourceNamespace);
      context.put(TITLE_WORDS, new HashSet<String>());
   }

   private void setWeight(Query q, String fieldName, boolean isExact) {
      if (!isExact) {
         if (Utils.FLD_KEYWORDS.equals(fieldName)) {
            q.setBoost(WEIGHT_KEYWORDS);
         }
         else if (fieldName.endsWith("Year")) {
            q.setBoost(WEIGHT_YEAR);
         }
         else if (fieldName.endsWith("Surname")) {
            q.setBoost(WEIGHT_SURNAME);
         }
         else if (fieldName.endsWith("Givenname")) {
            q.setBoost(WEIGHT_GIVEN_NAME);
         }
      }
   }

   protected void rewrite(TermRangeQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("CSRQ q="+q.toString()+" fld="+q.getField()+" lower="+q.getLowerVal()+" upper="+q.getUpperVal());
      String fieldName = q.getField();
      boolean isExact = !Utils.isEmpty(rb.req.getParams().get("exact"));
      if (!isExact) {
         // add year-only to dates
         if (fieldName.endsWith("Date")) {
            String lowerVal = q.getLowerTerm();
            if (lowerVal.length() > 4) {
               DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
               setWeight(q, fieldName, isExact);
               dq.add(q);
               Query temp = new TermQuery(new Term(fieldName, lowerVal.substring(0, 4)));
               temp.setBoost(q.getBoost() * BOOST_DATE_YEAR);
               dq.add(temp);
               out.add(dq, occur);
               q = null;
            }
         }
      }
      if (q != null) {
         setWeight(q, fieldName, isExact);
         out.add(q, occur);
      }
   }

   protected void rewrite(ConstantScorePrefixQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("CSPQ q="+q.toString()+" fld="+q.getPrefix().field()+" value="+q.getPrefix().text());
      String fieldName = q.getPrefix().field();
      boolean isExact = !Utils.isEmpty(rb.req.getParams().get("exact"));
      setWeight(q, fieldName, isExact);
      if (!isExact) {
         String text = q.getPrefix().text();
         // add title to keywords
         if (Utils.FLD_KEYWORDS.equals(fieldName)) {
            Query temp = new ConstantScorePrefixQuery(new Term(Utils.FLD_TITLE, text));
            temp.setBoost(q.getBoost() * BOOST_TITLE);
            out.add(temp, BooleanClause.Occur.SHOULD);
         }
      }
      out.add(q, occur);
   }

   protected void rewrite(WildcardQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      String fieldName = q.getTerm().field();
      boolean isExact = !Utils.isEmpty(rb.req.getParams().get("exact"));
      setWeight(q, fieldName, isExact);
      if (!isExact) {
         String text = q.getTerm().text();
         // add title to keywords
         if (Utils.FLD_KEYWORDS.equals(fieldName)) {
            Query temp = new WildcardQuery(new Term(Utils.FLD_TITLE, text));
            temp.setBoost(q.getBoost() * BOOST_TITLE);
            out.add(temp, BooleanClause.Occur.SHOULD);
         }
      }
      out.add(q, occur);
   }

   private boolean shouldExpandPlaces(ResponseBuilder rb) {
      // expand places if we're not querying the source namespace or the user has checked sup
      return (!((Boolean)rb.req.getContext().get(IS_SOURCE_NAMESPACE)) || !Utils.isEmpty(rb.req.getParams().get("sup")));
   }

   private void boostNosubordinate(String text, String fieldName, BooleanQuery out, ResponseBuilder rb) {
      // if we're querying the source namespace and the user has checked sub, then boost nosub
      if (((Boolean)rb.req.getContext().get(IS_SOURCE_NAMESPACE)) && !Utils.isEmpty(rb.req.getParams().get("sub"))) {
         TermQuery tq = new TermQuery(new Term(fieldName, text+","+Utils.NO_SUBPLACE)); // place has already been reversed, to add to end
         tq.setBoost(BOOST_PLACE_NOSUB);
         out.add(tq, BooleanClause.Occur.SHOULD);
      }
   }

   private float getPlaceBoost(String text, float origBoost) {
      char[] buf = text.toCharArray();
      int bufPos = buf.length;
      int level = PlaceExpandFilter.getPlaceLevel(buf, bufPos);
      return origBoost * BOOST_PLACE[Math.min(BOOST_PLACE.length,level)-1];
   }

   private void expandPlaceLevels(String text, String fieldName, float origBoost, Set<String> seenPlaces, Query out,
                                  PlaceSearcher placeSearcher, Analyzer analyzer)
   {
      Query temp;
      // strip off ,nosub if there is one
      if (text.endsWith(","+Utils.NO_SUBPLACE)) {
         text = text.substring(0, text.length() - (Utils.NO_SUBPLACE.length()+1));
      }
      // get ali's from place searcher
      List<String> locatedInPlaces = new ArrayList<String>();
      // argh - I need to re-reverse the levels to use the place searcher here, then use an analyzer to reverse the answers
      char[] src = text.toCharArray();
      char[] target = new char[src.length];
      int targetLen = PlaceReverseFilter.reverseBuffer(src, src.length, target);
      String place = new String(target, 0, targetLen);
      String[] indexResult = placeSearcher.getIndex(place);
      // ali's start at position 1
      for (int i = 1; i < indexResult.length; i++) {
         locatedInPlaces.add(PlaceRewriteComponent.getAnalyzedPlaceText(indexResult[i], analyzer));
      }
      // strip off last level to get primary located-in
      int pos = text.lastIndexOf(',');
      if (pos > 0) {
         locatedInPlaces.add(text.substring(0, pos));
      }

      // add located-in,nosub for each level of each located-in place
      StringBuilder placeBuf = new StringBuilder();
      for (String locatedInPlace : locatedInPlaces) {
         char[] buf = locatedInPlace.toCharArray();
         int bufPos = buf.length;
         int level = PlaceExpandFilter.getPlaceLevel(buf, bufPos);
         while (bufPos > 0) {
            placeBuf.setLength(0);
            placeBuf.append(buf, 0, bufPos);
            placeBuf.append(',');
            placeBuf.append(Utils.NO_SUBPLACE);
            place = placeBuf.toString();
            if (seenPlaces == null || seenPlaces.add(fieldName+":"+place)) {
               temp = new TermQuery(new Term(fieldName, place));
               float boost = origBoost * BOOST_PLACE[Math.min(BOOST_PLACE.length,level)-1];
               temp.setBoost(boost);
//logger.warning("expandPlaceLevels text="+text+" fieldName="+fieldName+" thisBoost="+thisBoost+" place="+place+" level="+level+" boost="+boost);
               Utils.addQueryClause(out, temp, BooleanClause.Occur.SHOULD);
            }
            bufPos = PlaceExpandFilter.getNextPlacePosition(buf, bufPos);
            level--;
         }
      }
   }

   protected void rewrite(PhraseQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("PQ q="+q.toString()+"term cnt="+q.getTerms().length+" fld[0]="+q.getTerms()[0].field()+" value[0]="+q.getTerms()[0].text());
      String fieldName = q.getTerms()[0].field();
      boolean isExact = !Utils.isEmpty(rb.req.getParams().get("exact"));
      setWeight(q, fieldName, isExact);
      if (!isExact) {
         if (Utils.FLD_KEYWORDS.equals(fieldName)) { // || (isSourceNamespace && (Utils.FLD_AUTHOR.equals(fieldName) || Utils.FLD_SOURCE_TITLE.equals(fieldName)))) {
            PhraseQuery temp = new PhraseQuery();
            Term[] terms = q.getTerms();
            int[] positions = q.getPositions();
            for (int i = 0; i < terms.length; i++) {
               temp.add(new Term(Utils.FLD_TITLE, terms[i].text()), positions[i]);
            }
            temp.setBoost(q.getBoost() * BOOST_TITLE);
            out.add(temp, BooleanClause.Occur.SHOULD);
         }
      }
      out.add(q, occur);
   }

   private void expandName(String fieldName, float origBoost, String text, DisjunctionMaxQuery dq,
                           Set<String> titleWords)
   {
      // boost to title
      titleWords.add(text);

      // add related names
      org.folg.names.search.Searcher searcher =
         (fieldName.endsWith("Surname") ? org.folg.names.search.Searcher.getSurnameInstance() : org.folg.names.search.Searcher.getGivennameInstance());
      for (String name : searcher.getAdditionalSearchTokens(text)) {
         if (!text.equals(name)) {
            TermQuery temp = new TermQuery(new Term(fieldName, name));
            temp.setBoost(origBoost * BOOST_NAME_RELATED);
            dq.add(temp);
         }
      }
      if ("PersonSurname".equals(fieldName)) {
         TermQuery temp = new TermQuery(new Term("PersonMarriedName", text));
         temp.setBoost(origBoost * BOOST_NAME_MARR);
         dq.add(temp);
      }
   }

   protected void rewrite(DisjunctionMaxQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      // Disjunction queries can happen only on Place, Surname, or Givenname fields (from PlaceRewriteComponent or VirtualFieldRewriteComponent)
      boolean isExact = !Utils.isEmpty(rb.req.getParams().get("exact"));
      if (!isExact) {
//         boolean isSourceNamespace = (Boolean)rb.req.getContext().get(IS_SOURCE_NAMESPACE);
         DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
         Set<String> seenPlaces = new HashSet<String>();
         Iterator i = q.iterator();
         while (i.hasNext()) {
            Query disjunct = (Query)i.next();
            if (disjunct instanceof TermQuery) {
               TermQuery tq = (TermQuery)disjunct;
               String fieldName = tq.getTerm().field();
               setWeight(tq, fieldName, isExact);
               String text = tq.getTerm().text();
               if (fieldName.endsWith("Place")) {
                  float origBoost = tq.getBoost();
                  float boost = getPlaceBoost(text, origBoost);
                  boostNosubordinate(text, fieldName, out, rb);
                  if (shouldExpandPlaces(rb)) {
                     Analyzer analyzer = rb.req.getSchema().getQueryAnalyzer();
                     PlaceSearcher placeSearcher = new PlaceSearcher(rb.req.getSearcher(), analyzer);
                     expandPlaceLevels(text, fieldName, origBoost, seenPlaces, dq, placeSearcher, analyzer);
                  }
                  if (seenPlaces.add(fieldName+":"+text)) {
                     tq.setBoost(boost);
                     dq.add(tq);
                  }
               }
               else if ((fieldName.endsWith("Surname") || fieldName.endsWith("Givenname")) &&
                        (!Utils.UNKNOWN_NAME.equals(text) && !Utils.LIVING_NAME.equals(text))) { // don't boost "unknown" or "living"
                  @SuppressWarnings("unchecked")
                  Set<String> titleWords = (Set<String>)rb.req.getContext().get(TITLE_WORDS);
                  expandName(fieldName, tq.getBoost(), text, dq, titleWords);
                  dq.add(tq); // add original term
               }
               else {
                  dq.add(tq);
               }
            }
         }
         out.add(dq, occur);
      }
      else {
         out.add(q, occur);
      }
   }

   protected void rewrite(TermQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("TQ q="+q.toString()+" fld="+q.getTerm().field()+" value="+q.getTerm().text());
      String fieldName = q.getTerm().field();
      boolean isExact = !Utils.isEmpty(rb.req.getParams().get("exact"));
      setWeight(q, fieldName, isExact);
      if (!isExact) {
         String text = q.getTerm().text();
         if (Utils.FLD_KEYWORDS.equals(fieldName)) {
            Query temp = new TermQuery(new Term(Utils.FLD_TITLE, text));
            temp.setBoost(q.getBoost() * BOOST_TITLE);
            out.add(temp, BooleanClause.Occur.SHOULD);
         }
//         else if (isSourceNamespace && (Utils.FLD_AUTHOR.equals(fieldName) || Utils.FLD_SOURCE_TITLE.equals(fieldName))) {
//            titleWords.add(text);
//         }
         // add 0000 to dates
         else if (fieldName.endsWith("Date")) {
            // add month to dates
            if (text.length() > 4) {
               DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
               dq.add(q);
               if (text.length() > 6) {
                  Query temp = new TermQuery(new Term(fieldName, text.substring(0, 6)));
                  temp.setBoost(q.getBoost() * BOOST_DATE_MONTH);
                  dq.add(temp);
               }
               Query temp = new TermQuery(new Term(fieldName, text.substring(0, 4)));
               temp.setBoost(q.getBoost() * BOOST_DATE_YEAR);
               dq.add(temp);
               out.add(dq, occur);
               q = null;
            }
         }
         // add unknown to names
         else if ((fieldName.endsWith("Surname") || fieldName.endsWith("Givenname")) &&
                  (!Utils.UNKNOWN_NAME.equals(text) && !Utils.LIVING_NAME.equals(text))) { // don't boost "unknown" or "living"
            DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
            @SuppressWarnings("unchecked")
            Set<String> titleWords = (Set<String>)rb.req.getContext().get(TITLE_WORDS);
            expandName(fieldName, q.getBoost(), text, dq, titleWords);
            dq.add(q); // add original term
            out.add(dq, occur); // add dismax query
            q = null;
         }
         // add a fuzzy term
         else if (fieldName.equals(Utils.FLD_PLACE_NAME)) {
            FuzzyQuery fq = new FuzzyQuery(q.getTerm(), MIN_FUZZY_SIMILARITY);
            out.add(fq, occur);
            q = null;
         }
         // expand places
         else if (fieldName.endsWith("Place")) {
            float origBoost = q.getBoost();
            float boost = getPlaceBoost(text, origBoost);
//            float b = boost * (isSourceNamespace ? BOOST_SUBORDINATE_SOURCE_PLACE  : 1.0f);
            q.setBoost(boost);
            boostNosubordinate(text, fieldName, out, rb);
            if (shouldExpandPlaces(rb)) {
               DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
               Analyzer analyzer = rb.req.getSchema().getQueryAnalyzer();
               PlaceSearcher placeSearcher = new PlaceSearcher(rb.req.getSearcher(), analyzer);
               expandPlaceLevels(text, fieldName, origBoost, null, dq, placeSearcher, analyzer);
               // add original term
               dq.add(q);
               // add dismax query
               out.add(dq, occur);
               q = null;
            }
         }
      }
      if (q != null) {
         out.add(q, occur);
      }
   }

   @SuppressWarnings({"unchecked"})
   protected void post(BooleanQuery out, ResponseBuilder rb) {
      Set<String> titleWords = (Set<String>)rb.req.getContext().get(TITLE_WORDS);
      for (String word : titleWords) {
         Query q = new TermQuery(new Term(Utils.FLD_TITLE, word));
         q.setBoost(BOOST_TITLE_WORD);
         out.add(q, BooleanClause.Occur.SHOULD);
      }
   }

   /////////////////////////////////////////////
   ///  SolrInfoMBean
   ////////////////////////////////////////////

   public String getDescription() {
     return "BoostRewriteComponent";
   }

   public String getVersion() {
     return "1.1";
   }

   public String getSourceId() {
     return "BoostRewriteComponent";
   }

   public String getSource() {
     return "BoostRewriteComponent";
   }

}
