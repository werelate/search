package org.werelate.search;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.ConstantScorePrefixQuery;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.index.Term;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.werelate.util.Utils;

import java.io.Serializable;
import java.util.logging.Logger;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.StringReader;
import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 29, 2008
 */
public class PlaceSearcher
{
   public static final float MISSING_LATLNG = 999.0f;
   public static final float ERROR_LATLNG_NOT_FOUND = 1.0f;
   public static final float ERROR_LATLNG_PLACE_NOT_FOUND = 2.0f;
   public static final float ERROR_LATLNG_AMBIGUOUS = 3.0f;
   public static final String PLACE_COLON = "place:"; // must be lowercase

   protected static Logger logger = Logger.getLogger("org.werelate.search");

   private static final int MAX_PLACES_READ = 300; // must be >= MAX_AUTOCOMPLETE_PLACES and MAX_LOCATED_IN_PLACES
   private static final int MAX_AUTOCOMPLETE_PLACES = 100; // change in autocomplete.js if you change it here
   private static final int MAX_LOCATED_IN_PLACES = 300;
   private static final int MAX_MATCH_RESULTS = 12;
   private static final float LATLNG_SUCCESS = 0.0f;
   private static final String USSTATES_MINUS_GEORGIA = "alabama|alaska|arizona|arkansas|california|colorado|connecticut|delaware|district of columbia|"+
           "florida|hawaii|idaho|illinois|indiana|iowa|kansas|kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|mississippi|missouri|"+
           "montana|nebraska|nevada|new hampshire|new jersey|new mexico|new york|north carolina|north dakota|ohio|oklahoma|oregon|pennsylvania|rhode island|"+
           "south carolina|south dakota|tennessee|texas|utah|vermont|virginia|washington|west virginia|wisconsin|wyoming";
   private static final String USSTATES = USSTATES_MINUS_GEORGIA+"|georgia";
   private static final Pattern[] FIX_PATTERNS_CLEANSE = {
      Pattern.compile(",(\\S)"),                                 // space after comma
      Pattern.compile("\\s\\s+"),                                // remove multiple spaces
      Pattern.compile(", (, )+"),                                // remove multiple commas
      Pattern.compile("(^[, ]+)|([, ]+$)"),                      // remove preceding or following commas
      Pattern.compile("\\s+tws?p\\.?,", Pattern.CASE_INSENSITIVE), // expand into (township)
      Pattern.compile("\\s+par\\.?,", Pattern.CASE_INSENSITIVE), // expand into parish
      Pattern.compile("\\s+co\\.?,", Pattern.CASE_INSENSITIVE),  // expand into county
      Pattern.compile("(^|, )("+USSTATES_MINUS_GEORGIA+")$", Pattern.CASE_INSENSITIVE), // add united states
      Pattern.compile("(^|, )u\\.?s\\.?(a\\.?)?$", Pattern.CASE_INSENSITIVE), // expand into united states
   };
   private static final String[] FIX_REPLACEMENTS_CLEANSE = {
      ", $1",
      " ",
      ", ",
      "",
      " (township),",
      " Parish,",
      " County,",
      "$1$2, United States",
      "$1United States",
   };
   private static final Pattern[] FIX_PATTERNS_END = {
      Pattern.compile("(<|&lt;)/?s(>|&gt;)|(<|&lt;)/?b(>|&gt;)|(<|&lt;)/?u(>|&gt;)|(<|&lt;)/?i(>|&gt;)"), // remove formating tags
      Pattern.compile("<|&lt;|>|&gt;"),                          // remove < > (implying uncertainty)
      Pattern.compile(", (\\d+|\\d+-\\d+)$"),                    // remove terminating US zipcodes 
      Pattern.compile(", (\\d+|\\d+-\\d+), "),                   // remove US zipcodes prior to end of string
      Pattern.compile("(^|, )u\\.?s\\.?(a\\.?)?$", Pattern.CASE_INSENSITIVE), // expand into united states - repeat after removing tags
   };
   private static final String[] FIX_REPLACEMENTS_END = {
      "",
      "",
      "",
      ", ",
      "$1United States",
   };
   private static final Pattern[] FIX_PATTERNS_US = {
      Pattern.compile("\\s+(county|parish), ("+USSTATES+")\\b", Pattern.CASE_INSENSITIVE), // remove county/parish for US places
      Pattern.compile("(ward|district|justice precinct|precinct)\\s\\d+, (.*, ("+USSTATES+"))\\b", Pattern.CASE_INSENSITIVE), // remove ward #, etc for US places
   };
   private static final String[] FIX_REPLACEMENTS_US = {
      ", $2",
      "$2",
   };
   private static final Pattern[] FIX_PATTERNS_DETAIL = {
      Pattern.compile("^(of|near|prob\\.?|poss\\.?|probably|possibly)\\s+", Pattern.CASE_INSENSITIVE), // remove preceding modifiers
      Pattern.compile("^\\d[^,]*,"),                             // remove addresses
      Pattern.compile("\\w*\\d\\w*"),                            // remove words containing numbers (e.g., UK, Can postal codes)
   };
   private static final String[] FIX_REPLACEMENTS_DETAIL = {
      "",
      "",
      "",
   };
   private static final Pattern USSTATES_PATTERN = Pattern.compile("("+USSTATES+")");

   private static final String INDEX_CACHE_NAME = "placeIndex";
   private static final String MATCH_CACHE_NAME = "placeMatch";
   private static final String LATLNG_CACHE_NAME = "placeLatLng";
   private static final String AUTOCOMPLETE_CACHE_NAME = "placeAutoComplete";

   private final SolrIndexSearcher searcher;
   private final Analyzer analyzer;
   // TODO someday change this to a solr generic cache?
   private static JCS indexCache;
   private static JCS matchCache;
   private static JCS latLngCache;
   private static JCS autoCompleteCache;

   private static class MatchResult implements Serializable {
      String[] titles = null;
      String error = null;
   }

   private static synchronized void ensureCache()
   {
      if (indexCache == null)
      {
         try
         {
            indexCache = JCS.getInstance(INDEX_CACHE_NAME);
            matchCache = JCS.getInstance(MATCH_CACHE_NAME);
            latLngCache = JCS.getInstance(LATLNG_CACHE_NAME);
            autoCompleteCache = JCS.getInstance(AUTOCOMPLETE_CACHE_NAME);
         } catch (CacheException e)
         {
            throw new RuntimeException("Couldn't instantiate cache: " + e.getMessage());
         }
      }
   }

   private static String cleanseText(String placeText) {
      String result = placeText;
      for (int i = 0; i < FIX_PATTERNS_CLEANSE.length; i++) {
         Matcher m = FIX_PATTERNS_CLEANSE[i].matcher(result);
         result = m.replaceAll(FIX_REPLACEMENTS_CLEANSE[i]);
      }
      return result;
   }

   private static String getLookupText(String placeText, boolean applyFixes) {
      String result = placeText;
      if (applyFixes) {
         result = cleanseText(placeText);
         // remove info that might be after a US state abbrev - must be done before expanding US state abbrevs
         for (int i = 0; i < FIX_PATTERNS_END.length; i++) {
            Matcher m = FIX_PATTERNS_END[i].matcher(result);
            result = m.replaceAll(FIX_REPLACEMENTS_END[i]);
         }
         // if place ends with US State abbrev, append state name, united states
         int pos = result.lastIndexOf(',');
         int endPos = result.length();
         if (pos >= 0 && pos < result.length()-1 && result.substring(pos+1).trim().equalsIgnoreCase("united states")) {
            endPos = pos;
            pos = result.lastIndexOf(',', pos-1);
         }
         if (pos < result.length()-1) {
            String lastLevel = result.substring(pos+1,endPos).trim();
            lastLevel = Utils.getUSStateFromAbbrev(lastLevel);
            if (lastLevel != null) {
               StringBuilder buf = new StringBuilder();
               if (pos > 0) {
                  buf.append(result.substring(0,pos));
                  buf.append(", ");
               }
               buf.append(lastLevel.toLowerCase());
               buf.append(", United States");
               result = buf.toString();
            }
         }
         // apply US patterns - must be done after expanding US state abbrevs and before removing addresses (because of ward/etc #)
         for (int i = 0; i < FIX_PATTERNS_US.length; i++) {
            Matcher m = FIX_PATTERNS_US[i].matcher(result);
            result = m.replaceAll(FIX_REPLACEMENTS_US[i]);
         }

         // remove addresses and modifiers such as "prob."
         for (int i = 0; i < FIX_PATTERNS_DETAIL.length; i++) {
            Matcher m = FIX_PATTERNS_DETAIL[i].matcher(result);
            result = m.replaceAll(FIX_REPLACEMENTS_DETAIL[i]);
         }
      }
      return result;
   }

   public PlaceSearcher(SolrIndexSearcher searcher, Analyzer analyzer) {
      this.searcher = searcher;
      this.analyzer = analyzer;
      ensureCache();
   }

   // ignore unknown and tokens with numbers
   // don't use a for loop here for efficiency
   private boolean isValidTerm(String s) {
      if (s.equals("unknown")) return false;
      return true;
   }

   private DocSet lookupPlaceLevel(List<String> locatedInPlaces, String placeName) {
//logger.warning("lookupPlaceLevel placeName="+placeName);
      DocSet ds = null;
      BooleanQuery bq = new BooleanQuery(true);
      try
      {
         boolean prefixQuery = false;
         if (placeName.endsWith("*")) {
            prefixQuery = true;
            placeName = placeName.substring(0, placeName.length()-1);
         }
         TokenStream ts = analyzer.reusableTokenStream(Utils.FLD_PLACE_NAME, new StringReader(placeName));
         CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
         boolean hasMoreTerms = ts.incrementToken();
         while (hasMoreTerms) {
            String termValue = new String(termAttr.buffer(), 0, termAttr.length());
//logger.warning("lookupPlaceLevel token="+termValue);
            hasMoreTerms = ts.incrementToken();
            if (isValidTerm(termValue)) {
               Query q;
               if (prefixQuery && !hasMoreTerms) { // last term of a prefix query
                  q = new ConstantScorePrefixQuery(new Term(Utils.FLD_PLACE_NAME, termValue));
               }
               else {
                  q = new TermQuery(new Term(Utils.FLD_PLACE_NAME, termValue));
               }
               bq.add(q, BooleanClause.Occur.MUST);
            }
         }
         ts.end();
         ts.close();

         if (bq.getClauses().length == 0) {
            logger.warning("lookupPlaceLevel no tokens");
            return null; // no valid tokens
         }

         if (locatedInPlaces != null && locatedInPlaces.size() > 0) {
            Query temp = null;
            if (locatedInPlaces.size() > 1) {
               temp = new BooleanQuery(true);
               for (String locatedInPlace : locatedInPlaces) {
                  // use PlaceTitle field because the query analyzer does what we want
                  ts = analyzer.reusableTokenStream(Utils.FLD_PLACE_TITLE, new StringReader(locatedInPlace));
                  termAttr = ts.addAttribute(CharTermAttribute.class);
                  if (ts.incrementToken()) {
                     String termString = new String(termAttr.buffer(), 0, termAttr.length());
                     ((BooleanQuery)temp).add(new TermQuery(new Term(Utils.FLD_LOCATED_IN_PLACE, termString)), BooleanClause.Occur.SHOULD);
                  }
                  ts.end();
                  ts.close();
               }
            }
            else {
               // use PlaceTitle field because the query analyzer does what we want
               ts = analyzer.reusableTokenStream(Utils.FLD_PLACE_TITLE, new StringReader(locatedInPlaces.get(0)));
               termAttr = ts.addAttribute(CharTermAttribute.class);
               if (ts.incrementToken()) {
                  String termString = new String(termAttr.buffer(), 0, termAttr.length());
                  temp = new TermQuery(new Term(Utils.FLD_LOCATED_IN_PLACE, termString));
               }
               ts.end();
               ts.close();
            }
            if (temp != null) {
               bq.add(temp, BooleanClause.Occur.MUST);
            }
         }
         ds = searcher.getDocSet(bq);
      }
      catch (IOException e)
      {
         logger.warning("Exception looking up place: "+placeName+" in "+(locatedInPlaces.size() == 0 ? "n/a" : locatedInPlaces.get(0))+" error="+e.getMessage());
      }

      return ds;
   }

   private Document lookupPlaceByTitle(String placeText) {
      try
      {
         Token startToken = new Token();
         TokenStream ts = analyzer.reusableTokenStream(Utils.FLD_PLACE_TITLE, new StringReader(placeText));
         CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
         if (ts.incrementToken()) {
            String termValue = new String(termAttr.buffer(), 0, termAttr.length());
            int docNum = searcher.getFirstMatch(new Term(Utils.FLD_PLACE_TITLE, termValue));
            if (docNum >= 0) {
               return searcher.doc(docNum);
            }
         }
         ts.end();
         ts.close();
      } catch (IOException e)
      {
         logger.warning("lookupPlaceByTitle failed: " + placeText);
      }
//logger.warning("lookupPlaceByTitle placeText="+placeText+(doc == null ? " found" : " not found"));
      return null;
   }

   private Document[] readPlaces(DocSet ds, int max) throws IOException
   {
      Document[] docs = null;
      if (ds.size() > 0) {
         List<Document> temp = new ArrayList<Document>();
         DocIterator i = ds.iterator();
         while (i.hasNext() && temp.size() < max) {
            Document doc = searcher.doc(i.nextDoc());
            temp.add(doc);
         }
         if (temp.size() > 0) {
            docs = new Document[temp.size()];
            temp.toArray(docs);
         }
      }
      return docs;
   }

//   private Document readTopPlace(DocSet ds, String placeName) throws IOException
//   {
//      Document topDoc = null;
//      int shortestLength = Integer.MAX_VALUE;
//      boolean exactMatch = false;
//      placeName = placeName+","; // to test exact matches
//
//      DocIterator i = ds.iterator();
//      int j = 0;
//      while (i.hasNext()) {
//         if (j++ > MAX_PLACES_READ) {
//            break;
//         }
//         Document doc = searcher.doc(i.nextDoc());
//         String title = doc.getFieldable(Utils.FLD_TITLE_STORED).stringValue();
//         if (Utils.startsWithIgnoreCase(title, placeName)) {
//            if (!exactMatch || title.length() < shortestLength || title.equals(GEORGIA)) { // Georgia should always float to the top
//               topDoc = doc;
//               shortestLength = title.length();
//               exactMatch = true;
//            }
//         }
//         else if (!exactMatch && title.length() < shortestLength) {
//            topDoc = doc;
//            shortestLength = title.length();
//         }
//      }
//      return topDoc;
//   }

   private static class TitleStringComparator implements Comparator<String> {
      public int compare(String o, String o1)
      {
         int diff = o.length() - o1.length();
         if (diff == 0) {
            diff = o.compareTo(o1);
         }
         return diff;
      }
   }

   private String getKey(Document doc, boolean prepare, String placeName, String placeNameComma) {
      String key = doc.getFieldable(Utils.FLD_TITLE_STORED).stringValue();
      if (prepare) {
         key = key.toLowerCase();
         String saveKey = key;
         if (!saveKey.endsWith(", united states")) {
            key += "foreign-------------"; // make foreign keys sort after US keys
         }
         else if (saveKey.equals("georgia, united states")) {
            key = "georgia";  // make georgia be at the same level as georgia (country)
         }
         if (!saveKey.equals(placeName) && !saveKey.startsWith(placeNameComma)) { // make inexact matches sort after exact matches
            key += "alternate name match-----";
         }
      }
      return key;
   }

   /**
    * Return the shortest places
    * @param ds
    * @param max
    * @param placeName - should be lowercase
    * @return
    * @throws IOException
    */
   private Document[] readTopPlaces(DocSet ds, int max, String placeName) throws IOException
   {
      int bestLevel = 99;
      String placeNameComma = placeName+",";
      Document[] docs = null;

      if (ds.size() > 0) {
         Map<String, Document>docMap = new TreeMap<String, Document>(new TitleStringComparator());
         DocIterator i = ds.iterator();
         int j = 0;
//logger.warning("placeName="+placeName+" max="+max);
         while (i.hasNext()) {
            if (j > MAX_PLACES_READ) {
               break;
            }
            Document doc = searcher.doc(i.nextDoc());
            String key = getKey(doc, true, placeName, placeNameComma);
            int level = Utils.countOccurrences(',', key);
//logger.warning("j="+j+" key="+key+" level="+level+" bestLevel="+bestLevel);
            if (level < bestLevel) {
               bestLevel = level;
               docMap.clear();
            }
            if (level == bestLevel) {
               docMap.put(key, doc);
            }
            j++;
         }
         if (docMap.size() > 0) {
            docs = new Document[docMap.size() > max ? max : docMap.size()];
            j = 0;
            for (Document doc : docMap.values()) {
               docs[j++] = doc;
//logger.warning("j="+j+" title="+getFieldValue(doc, Utils.FLD_TITLE_STORED));
               if (j == docs.length) {
                  break;
               }
            }
         }
      }
      return docs;
   }

   private static class LookupResult {
      Document[] docs;
      String standardizedTitle;
      String nearestLatitude;
      String nearestLongitude;
   }

   // returns null if field value not present
   private String getFieldValue(Document doc, String fieldName) {
      Fieldable f = doc.getFieldable(fieldName);
      if (f != null) {
         return f.stringValue();
      }
      return null;
   }

   private LookupResult lookupPlace(String placeText, int max, boolean getTopPlaces, boolean requireCountryStateMatch) {
      StringBuilder standardizedTitle = new StringBuilder();
      String latitude = null;
      String longitude = null;
      Document[] docs = null;

      // break up the placetext into levels
      String[] levels = placeText.split("\\s*,\\s*");

      try
      {
         List<String> locatedInPlaces = new ArrayList<String>();
//logger.warning("lookupPlace placeText="+placeText);
         // look up right-to-left
         for (int i = levels.length-1; i >= 0; i--) {
            // short-cut for very common case
            String lcLevel = levels[i].toLowerCase();
            if (i == levels.length-1 && lcLevel.equals("united states")) {
               String title = "United States";
               standardizedTitle.append(title);
               locatedInPlaces.add(title);
               continue;
            }

            // handle X, , Y by skipping the intermediate level
            if (levels[i].length() == 0) {
               continue;
            }

            // look up each level one at a time
            docs = null;
            // short-cut for US states
            if (i == levels.length-2 && levels[i+1].equalsIgnoreCase("united states")) {
               Matcher m = USSTATES_PATTERN.matcher(lcLevel);
               if (m.matches()) {
                  String title = m.group()+", United States";
                  Document d = lookupPlaceByTitle(title);
                  if (d != null) {
                     docs = new Document[1];
                     docs[0] = d;
                  }
//logger.warning("lookupPlace short-cut title="+title+" found="+(d == null ? "false" : "true"));
               }
            }
            if (docs == null) { // no short-cut
               DocSet ds = lookupPlaceLevel(locatedInPlaces, lcLevel);
//logger.warning("lookupPlace level="+i+" locatedInPlaces="+ locatedInPlaces.size()+" place="+levels[i]+" count="+(ds == null ? 0 : ds.size()));
               if (ds != null && ds.size() > 0) {
                  if (getTopPlaces) {
                     docs = readTopPlaces(ds, i == 0 ? max : MAX_LOCATED_IN_PLACES, lcLevel);
                  }
                  else { // get places, but don't prefer highest-level matches
                     docs = readPlaces(ds, i == 0 ? max : MAX_LOCATED_IN_PLACES);
                  }
               }
            }
            // the outermost level must match places at level 0 or 1 (avoids false-matching "County, State, bogus stuff")
            if (requireCountryStateMatch && i == levels.length-1 && docs != null && Utils.countOccurrences(',', getKey(docs[0], false, null, null)) > 1) {
               docs = null;
            }
            if (docs != null) {
               Document d = docs[0];

               // set default place title
               String title = d.getFieldable(Utils.FLD_TITLE_STORED).stringValue();
               standardizedTitle.setLength(0);
               standardizedTitle.append(title);

               // set default lat+long
               String l = getFieldValue(d, Utils.FLD_LATITUDE);
               if (l != null) {
                  latitude = l;
               }
               l = getFieldValue(d, Utils.FLD_LONGITUDE);
               if (l != null) {
                  longitude = l;
               }

               // set located-in places
               if (i > 0) {
                  locatedInPlaces.clear();
                  for (Document doc : docs) {
                     locatedInPlaces.add(doc.getFieldable(Utils.FLD_TITLE_STORED).stringValue());
                  }
               }
            }
            else {
               // set default place title
               if (standardizedTitle.length() > 0) {
                  standardizedTitle.insert(0, ", ");
               }
               standardizedTitle.insert(0, Utils.capitalizePlaceLevel(levels[i]));
            }
         }
      }
      catch (IOException e)
      {
         logger.warning("Exception looking up: " + placeText + " exception="+e.getMessage());
      }

      LookupResult result = new LookupResult();
      result.docs = docs;
      result.standardizedTitle = standardizedTitle.toString();
      result.nearestLatitude = latitude;
      result.nearestLongitude = longitude;
      return result;
   }

   private void addIndexCache(String placeTitle, String[] indexResult) {
      try
      {
         indexCache.put(placeTitle, indexResult);
      } catch (CacheException e)
      {
         logger.warning("Couldn't add "+placeTitle+" to indexCache: " + e.getMessage());
      }
   }

   /**
    * Call this function to get tokens to index for a known title
    * @param placeTitle
    * @return place title of final redir and any ali's
    */
   public String[] getIndex(String placeTitle) {
      String[] result = (String[]) indexCache.get(placeTitle);
      if (result == null) {
         Document doc = lookupPlaceByTitle(placeTitle);
         if (doc != null) {
            result = getIndexResultFromDocument(doc);
         }
         else {
            result = new String[1];
            result[0] = placeTitle;
         }

         addIndexCache(placeTitle, result);
      }

      return result;
   }

   private String[] getIndexResultFromDocument(Document doc) {
      Fieldable[] alis = doc.getFieldables(Utils.FLD_LOCATED_IN_PLACE_STORED);
      String[] result = new String[(alis != null ? alis.length : 0) + 1];
      result[0] = getFieldValue(doc, Utils.FLD_TITLE_STORED);
      if (alis != null) {
         int i = 1;
         for (Fieldable ali : alis) {
            result[i++] = ali.stringValue();
         }
      }
      return result;
   }

   /**
    * Return standardized place text and optional error
    * @param placeText to standardize
    * @return array of size 2; [0] = standardized text; [1] = error
    */
   public String[] standardize(String placeText, String defaultCountry) {
      String[] result = new String[2];
      MatchResult matchResult = getMatchResult(placeText, defaultCountry);
      result[0] = matchResult.titles != null && matchResult.titles.length > 0 ? matchResult.titles[0] : "";
      result[1] = matchResult.error;
      return result;
   }

   public String[] getMatches(String placeText) {
      MatchResult mr = getMatchResult(placeText, null);
      if (mr.error == null) {
         return mr.titles;
      }
      else {
         return null;
      }
   }

   /**
    * Find matching titles for a specified place text
    * @param placeText to standardize
    * @return MatchResult, including array of 1 or more matching titles, plus a possible "missing" error
    */
   private MatchResult getMatchResult(String placeText, String defaultCountry) {
      String lookupText = getLookupText(placeText, true);
      String matchCacheKey = lookupText.toLowerCase()+(Utils.isEmpty(defaultCountry) ? "" : "|"+defaultCountry);
//logger.warning("get lookup text="+lookupText);

      MatchResult matchResult = (MatchResult) matchCache.get(matchCacheKey);
      if (matchResult == null) {
         matchResult = new MatchResult();
         // lookup by title first
         Document placeDoc = lookupPlaceByTitle(lookupText);
         if (placeDoc != null) {
            matchResult.titles = new String[1];
            String placeTitle = placeDoc.getFieldable(Utils.FLD_TITLE_STORED).stringValue();
//logger.warning("lookup by title found="+placeTitle);
            matchResult.titles[0] = placeTitle;
            addIndexCache(placeTitle, getIndexResultFromDocument(placeDoc));
         }
         else { // if not found by title, lookup top-down
            LookupResult lr = lookupPlace(lookupText, MAX_MATCH_RESULTS, true, true);
//logger.warning("lookup top-down count="+(lr.docs == null ? 0 : lr.docs.length));
            if ((lr.docs == null || lr.docs.length == 0) && !Utils.isEmpty(defaultCountry)) {
               lr = lookupPlace(getLookupText(placeText+", "+defaultCountry, true), MAX_MATCH_RESULTS, true, true);
            }
            if (lr.docs == null || lr.docs.length == 0) { // if don't find any, then try again without requiring countrystate match
               lr = lookupPlace(lookupText, MAX_MATCH_RESULTS, true, false);
            }
            if (lr.docs != null && lr.docs.length > 0) {
               matchResult.titles = new String[lr.docs.length];
               int i = 0;
               for (Document doc : lr.docs) {
                  String placeTitle = doc.getFieldable(Utils.FLD_TITLE_STORED).stringValue();
//logger.warning("lookup top-down i="+i+" title="+placeTitle);
                  matchResult.titles[i] = placeTitle;
                  addIndexCache(placeTitle, getIndexResultFromDocument(doc));
                  i++;
               }
            }
            else {
               matchResult.titles = new String[1];
               matchResult.error = "missing";
               matchResult.titles[0] = lr.standardizedTitle;
               logger.info("Not found title="+lr.standardizedTitle);
               addIndexCache(lr.standardizedTitle, matchResult.titles);
            }
         }

         try
         {
            matchCache.put(matchCacheKey, matchResult);
         } catch (CacheException e)
         {
            logger.warning("Couldn't add to matchCache: " + e.getMessage());
         }
      }

      return matchResult;
   }

   private String getParentPlaceTitle(String placeTitle) {
      int commaPos = placeTitle.indexOf(',');
      if (commaPos > 0 && commaPos < placeTitle.length()) {
         return placeTitle.substring(commaPos+1);
      }
      return null;
   }

   private float[] appendError(float[] results, float error) {
      if (results == null) {
         results = new float[3];
         results[0] = MISSING_LATLNG;
         results[1] = MISSING_LATLNG;
      }
      else if (results.length == 2) {
         float[] temp = results;
         results = new float[3];
         System.arraycopy(temp, 0, results, 0, 2);
      }
      results[2] = error;
      return results;
   }

   // if place not found at all, return null; may also return float[] w/ ERROR_LATLNG_NOT_FOUND
   private float[] getLatLngByTitle(String lookupText, int recursionLevel, boolean isKnownTitle) {
//logger.warning("getLatLngByTitle lookupText="+lookupText+" recursionLevel="+recursionLevel);
      Document doc = lookupPlaceByTitle(lookupText);
      if (doc == null) {
         if (isKnownTitle) {
            lookupText = getParentPlaceTitle(lookupText);
            if (lookupText != null) {
               return appendError(getLatLngByTitle(lookupText, recursionLevel, isKnownTitle), ERROR_LATLNG_PLACE_NOT_FOUND);
            }
            else {
               return appendError(null, ERROR_LATLNG_PLACE_NOT_FOUND);
            }
         }
         else {
            return null;
         }
      }

      float latitude = MISSING_LATLNG;
      float longitude = MISSING_LATLNG;

      String s = getFieldValue(doc, Utils.FLD_LATITUDE);
      if (s != null) {
         try {
            latitude = Float.parseFloat(s);
         }
         catch (NumberFormatException e) {
            //
         }
      }
      s = getFieldValue(doc, Utils.FLD_LONGITUDE);
      if (s != null) {
         try {
            longitude = Float.parseFloat(s);
         }
         catch (NumberFormatException e) {
            //
         }
      }
      if (latitude == MISSING_LATLNG || longitude == MISSING_LATLNG) {
//logger.warning("Missing latitude="+latitude+" longitude="+longitude);
         lookupText = getParentPlaceTitle(lookupText);
         if (lookupText != null) {
            return appendError(getLatLngByTitle(lookupText, recursionLevel, isKnownTitle), ERROR_LATLNG_NOT_FOUND);
         }
         else {
            return appendError(null, ERROR_LATLNG_NOT_FOUND);
         }
      }

      float[] results = new float[2];
      results[0] = latitude;
      results[1] = longitude;

      return results;
   }

   /**
    * Lookup lat+lng
    * @param placeText
    * @param isKnownTitle - placeText is known to be a title
    * @return
    */
   public float[] getLatLng(String placeText, boolean isKnownTitle) {
      String lookupText = getLookupText(placeText, !isKnownTitle).toLowerCase();
//logger.warning("getLatLng placeText="+placeText+" lookupText="+lookupText);
      float[] results = (float[]) latLngCache.get(lookupText);
      if (results == null) {
         // first try looking up by title
         results = getLatLngByTitle(lookupText, 0, isKnownTitle);
         if (results == null) {
//logger.warning("lookup top-down");
            // try looking top-down
            float latitude = MISSING_LATLNG;
            float longitude = MISSING_LATLNG;
            float error = ERROR_LATLNG_NOT_FOUND;

            if (isKnownTitle) {
               error = ERROR_LATLNG_PLACE_NOT_FOUND; // we should have found it above
            }
            else {
               LookupResult lr = lookupPlace(lookupText, 2, true, true);

               if (lr.nearestLatitude != null) {
                  try {
                     latitude = Float.parseFloat(lr.nearestLatitude);
                  }
                  catch (NumberFormatException e) {
                     // ignore
                  }
               }
               if (lr.nearestLongitude != null) {
                  try {
                     longitude = Float.parseFloat(lr.nearestLongitude);
                  }
                  catch (NumberFormatException e) {
                     // ignore
                  }
               }
//logger.warning("lat="+latitude+" lng="+longitude);
               if (lr.docs != null && lr.docs.length > 0) {
                  int i = 0;
                  for (Document doc : lr.docs) {
                     String title = getFieldValue(doc, Utils.FLD_TITLE_STORED);
//logger.warning("getlatlng i="+i+" title="+title+" lookupText="+lookupText);
                     if (i == 0) {
                        if (getFieldValue(doc, Utils.FLD_LATITUDE) != null && getFieldValue(doc, Utils.FLD_LONGITUDE) != null) { // found latlng
                           error = LATLNG_SUCCESS;
                        }
                     }
                     else { // result is ambiguous if second doc starts with the same name as the lookup text
//logger.warning("getLatLng title="+title);
                        int pos1 = title.indexOf(',');
                        if (pos1 < 0) pos1 = title.length();
                        int pos2 = lookupText.indexOf(',');
                        if (pos2 < 0) pos2 = lookupText.length();
                        if (title.substring(0, pos1).equalsIgnoreCase(lookupText.substring(0, pos2))) {
                           error = ERROR_LATLNG_AMBIGUOUS;
                        }
                     }
                     i++;
                  }
               }
               else {
                  error = ERROR_LATLNG_PLACE_NOT_FOUND;
               }
            }

            if (error == LATLNG_SUCCESS) {
               results = new float[2];
            }
            else {
               results = new float[3];
               results[2] = error;
            }
            results[0] = latitude;
            results[1] = longitude;
         }

         if (!isKnownTitle) { // don't cache known titles; they're cached on the wiki server
            try
            {
               latLngCache.put(lookupText, results);
            } catch (CacheException e)
            {
               logger.warning("Couldn't add to latLngCache: " + e.getMessage());
            }
         }
      }

      return results;
   }

   public String[][] getAutoComplete(String placeText) {
      String lookupText = getLookupText(placeText, false).toLowerCase();

      String[][] results = (String[][]) autoCompleteCache.get(lookupText);
      if (results == null) {
         LookupResult lr = lookupPlace(lookupText, MAX_AUTOCOMPLETE_PLACES, false, false);
         results = new String[lr.docs == null ? 0 : lr.docs.length][];
//logger.warning("getAutoComplete="+lookupText+" results="+results.length);
         if (lr.docs != null && lr.docs.length > 0) {
            int i = 0;
            for (Document doc : lr.docs) {
               String[] temp = new String[2];
               temp[0] = getFieldValue(doc, Utils.FLD_TITLE_STORED);
               temp[1] = getFieldValue(doc, Utils.FLD_PLACE_TYPE);
               results[i++] = temp;
            }
         }

         try
         {
            autoCompleteCache.put(lookupText, results);
         } catch (CacheException e)
         {
            logger.warning("Couldn't add to autoCompleteCache: " + e.getMessage());
         }
      }

      return results;
   }
}
