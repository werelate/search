package org.werelate.indexer;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.OperationTimeoutException;
import nu.xom.Element;
import nu.xom.Elements;
import nu.xom.Nodes;
import nu.xom.ParsingException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.solr.common.SolrInputDocument;
import org.werelate.util.HttpClientHelper;
import org.werelate.util.Utils;

import java.io.IOException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: dallan
 */
public class PlaceStandardizer {
   private static final Logger logger = Logger.getLogger("org.werelate.indexer");
   public static final String MC_PREFIX = "redir|";
   private static final int MC_TTL = 28800;
   private static final int PLACE_FACET_LEVELS = 3;

   private static final Set<String> PLACE_FIELDS = new HashSet<String>();
   static {
      PLACE_FIELDS.add(Utils.FLD_PERSON_BIRTH_PLACE);
      PLACE_FIELDS.add(Utils.FLD_PERSON_DEATH_PLACE);
      PLACE_FIELDS.add(Utils.FLD_MARRIAGE_PLACE);
      PLACE_FIELDS.add(Utils.FLD_OTHER_PLACE);
      PLACE_FIELDS.add(Utils.FLD_LOCATED_IN_PLACE);
//      PLACE_FIELDS.add(Utils.FLD_HUSBAND_BIRTH_PLACE);
//      PLACE_FIELDS.add(Utils.FLD_HUSBAND_DEATH_PLACE);
//      PLACE_FIELDS.add(Utils.FLD_WIFE_BIRTH_PLACE);
//      PLACE_FIELDS.add(Utils.FLD_WIFE_DEATH_PLACE);
   }
   private static final int MAX_REDIR_LOOKUPS = 50;

   private String indexUrl;
   private String wikiHostname;
   private MemcachedClient memcache;
   private HttpClientHelper indexClient;
   private HttpClientHelper wikiClient;

   public PlaceStandardizer(String indexUrl, MemcachedClient memcache, String wikiHostname, HttpClientHelper wikiClient) {
      this.indexUrl = indexUrl;
      this.memcache = memcache;
      this.wikiHostname = wikiHostname;
      this.wikiClient = wikiClient;
      indexClient = new HttpClientHelper(false);
   }

   // TODO need to move memcache lookup to wiki, and delete memcache upon place rename in hooks
   private Map<String,String> getFinalRedirectTargets(Set<String> places) throws IOException, ParsingException {
      Map<String,String> targets = new HashMap<String,String>();
      Set<String> titles = new HashSet<String>();

      // find places in cache
      for (String place : places) {
         String title = "Place:"+place;
         String key = Utils.getMemcacheKey(MC_PREFIX, title);
         String target = null;
         try {
            target = (String)memcache.get(key);
         }
         catch (OperationTimeoutException e) {
            logger.warning("Memcached timed out key="+key+" title="+title);
         }
         if (target != null) {
            if (target.length() == 0) {
               target = place;
            }
            targets.put(place, target);
         }
         else {
            titles.add(title);
         }
      }

      while (titles.size() > 0) {
         // send up to MAX_REDIR_LOOKUPS at a time
         Set<String> sending = new HashSet<String>();
         int cnt = 0;
         Iterator<String> iter = titles.iterator();
         while (iter.hasNext()) {
            String title = iter.next();
            iter.remove();
            sending.add(title);
            if (++cnt == MAX_REDIR_LOOKUPS) {
               break;
            }
         }

         GetMethod m = new GetMethod(Utils.getWikiAjaxUrl(wikiHostname, "wfGetPageRedirects", sending));
         try
         {
            wikiClient.executeHttpMethod(m);
            int statusCode = m.getStatusCode();
            if (statusCode != 200) {
               throw new RuntimeException("Unexpected http status code="+statusCode+" for titles="+Utils.join("|", sending));
            }
            String response=HttpClientHelper.getResponse(m);
            Elements pages = null;
            if (Utils.isEmpty(response)) {
               logger.warning("Unexpected empty response for pages="+Utils.join("|", sending));
            }
            else {
               Element root = wikiClient.parseText(response).getRootElement();
               if (Integer.parseInt(root.getAttributeValue("status")) != HttpClientHelper.STATUS_OK) {
                  throw new RuntimeException("Unexpected status="+root.getAttributeValue("status")+" for titles="+Utils.join("|", sending));
               }
               pages = root.getChildElements("page");
            }

            // add redirects
            if (pages != null) { // if we got an empty response, assume no redirects.  this isn't always a correct assumption, but it should be a rare mistake
               for (int i=0; i < pages.size(); i++)
               {
                  Element page = pages.get(i);
                  String sourceTitle = page.getAttributeValue("source");
                  String source = sourceTitle.substring("Place:".length());
                  String targetTitle = page.getAttributeValue("target");
                  String target = (targetTitle.startsWith("Place:") ? targetTitle.substring("Place:".length()) : source);
                  targets.put(source, target);
                  memcache.set(Utils.getMemcacheKey(MC_PREFIX, sourceTitle), MC_TTL, target);
               }
            }

            // add non-redirects
            for (String title : sending) {
               String place = title.substring("Place:".length());
               if (targets.get(place) == null) {
                  targets.put(place,place);
                  memcache.set(Utils.getMemcacheKey(MC_PREFIX, title), MC_TTL, "");
               }
            }
         }
         finally
         {
            m.releaseConnection();
         }
      }

      return targets;
   }

   private String prepareToFacet(String place) {
      StringBuilder buf = new StringBuilder();
      String[] levels = place.split(",");
      for (int i = levels.length - 1; i >= 0; i--) {
         if (buf.length() > 0) {
            buf.append(',');
         }
         buf.append(levels[i].trim());
      }
      buf.append(',');
      buf.append(Utils.NO_SUBPLACE);
      return buf.toString();
   }

   private void addFacetValues(Set<String>[] facets, String place) {
      place = prepareToFacet(place);
      int pos = 0;
      for (int i = 0; i < PLACE_FACET_LEVELS; i++) {
         // facet on county level for United States only
         if (i == PLACE_FACET_LEVELS-1 && !place.startsWith("United States,")) {
            break;
         }
         pos = place.indexOf(',', pos);
         String facetValue = (pos < 0 ? place : place.substring(0,pos));
         if (i == 0 && !Utils.COUNTRIES.contains(facetValue)) {
            break; // don't facet on non-standardized place text - could be garbage
         }
         facets[i].add(facetValue);
         if (pos < 0) {
            break;
         }
         else {
            pos++;
         }
      }
   }

   private void addFacets(SolrInputDocument doc, String fldName, Set<String> facetValues) {
      for (String facetValue : facetValues) {
         doc.addField(fldName, facetValue);
      }
   }

   public void standardizePlaces(List<SolrInputDocument> docs) throws IOException, ParsingException {
      // gather all of the places
      Set<String> places = new HashSet<String>();
      for (SolrInputDocument doc : docs) {
         for (String fieldName : doc.getFieldNames()) {
            if (PLACE_FIELDS.contains(fieldName)) {
               for (Object value : doc.getFieldValues(fieldName)) {
                  String place = (String)value;
                  if (!Utils.UNKNOWN_PLACE.equals(place)) {
                     places.add((String)value);
                  }
               }
            }
         }
      }

      // get redirects
      Map<String,String> redirectTargets = getFinalRedirectTargets(places);

      // standardize all fields but FLD_LOCATED_IN_PLACE
      places.clear();
      @SuppressWarnings({"unchecked"})
      Set<String>[] facets = (Set<String>[])new HashSet[PLACE_FACET_LEVELS];
      for (int i = 0; i < PLACE_FACET_LEVELS; i++) {
         facets[i] = new HashSet<String>();
      }
      for (SolrInputDocument doc : docs) {
         for (Set<String> facet : facets) {
            facet.clear();
         }
         for (String fieldName : doc.getFieldNames()) {
            if (PLACE_FIELDS.contains(fieldName)) {
               for (Object value : doc.getFieldValues(fieldName)) {
                  String place = (String)value;
                  if (!Utils.UNKNOWN_PLACE.equals(place)) {
                     place = redirectTargets.get((String) value);
                     // generate place0,1,2 values
                     addFacetValues(facets, place);

                     // standardize all fields but fld_located_in_place
                     if (!fieldName.equals(Utils.FLD_LOCATED_IN_PLACE)) {
                        places.add(place);
                     }
                  }
               }
            }
            else if (Utils.FLD_PLACE_TITLE.equals(fieldName)) {
               addFacetValues(facets, (String)doc.getFieldValue(fieldName));
            }
         }

         // add facets
         addFacets(doc, Utils.FLD_PLACE0, facets[0]);
         addFacets(doc, Utils.FLD_PLACE1, facets[1]);
         addFacets(doc, Utils.FLD_PLACE2, facets[2]);
      }

      // call index server to get place tokens to index
      Map<String,String[]> placeIndexTokens = new HashMap<String,String[]>();
      if (places.size() > 0) {
         PostMethod m = new PostMethod(indexUrl+"/placeindex");
         String q = Utils.join("|", places);
         m.addParameter("q", q);
         Element root = null;
         try
         {
            indexClient.executeHttpMethod(m);
            String response = HttpClientHelper.getResponse(m);
            root = indexClient.parseText(response).getRootElement();
         }
         finally
         {
            m.releaseConnection();
         }

         if (root != null) {
            // check for valid status
            Nodes status = root.query("/response/lst[@name='responseHeader']/int[@name='status']");
            if (status.size() == 0 || Integer.parseInt(status.get(0).getValue()) != 0) {
               throw new RuntimeException("Unexpected status="+(status.size() == 0 ? "not found" : status.get(0).getValue()));
            }

            // collect tokens to index
            Nodes results = root.query("/response/arr[@name='response']/lst");
            for (int i = 0; i < results.size(); i++) {
               Element result = (Element)results.get(i);
               Elements children = result.getChildElements();
               String placeName = null;
               String[] indexTokens = null;
               for (int j = 0; j < children.size(); j++) {
                  Element child = children.get(j);
                  if (child.getAttributeValue("name").equals("q")) {
                     placeName = child.getValue();
                  }
                  else if (child.getAttributeValue("name").equals("index")) {
                     Elements tokens = child.getChildElements();
                     if (tokens.size() > 0) {
                        indexTokens = new String[tokens.size()];
                        for (int k = 0; k < tokens.size(); k++) {
                           indexTokens[k] = tokens.get(k).getValue();
                        }
                     }
                  }
               }
               if (placeName != null && indexTokens != null) {
                  placeIndexTokens.put(placeName, indexTokens);
               }
            }
         }
      }

      // update documents
      Map<String,List<String>> placeFieldTokens = new HashMap<String,List<String>>();
      for (SolrInputDocument doc : docs) {
         placeFieldTokens.clear();
         for (String fieldName : doc.getFieldNames()) {
            if (PLACE_FIELDS.contains(fieldName)) {
               List<String> tokens = new ArrayList<String>();
               for (Object value : doc.getFieldValues(fieldName)) {
                  String place = (String)value;
                  if (!Utils.UNKNOWN_PLACE.equals(place)) {
                     place = redirectTargets.get(place);
                     if (fieldName.equals(Utils.FLD_LOCATED_IN_PLACE)) {
                        tokens.add(place);
                     }
                     else {
                        String[] indexTokens = placeIndexTokens.get(place);
                        if (indexTokens != null) {
                           boolean firstToken = true;
                           for (String token : indexTokens) {
                              if (firstToken) { // the first token is the exact place; the others are also-located-in's
                                 // TODO we could improve this by not adding nosub to leaf places; say places at level 4 or more
                                 // but we'd have to be sure to not add nosub to leaf queries either
                                 token = Utils.NO_SUBPLACE+", "+place;
                                 firstToken = false;
                              }
                              tokens.add(token);
                           }
                        }
                     }
                  }
               }
               placeFieldTokens.put(fieldName, tokens);
            }
         }
         for (Map.Entry<String,List<String>> entry : placeFieldTokens.entrySet()) {
            String fieldName = entry.getKey();
            List<String> tokens = entry.getValue();
            doc.removeField(fieldName);
            if (tokens.size() > 0) {
               for (String token : entry.getValue()) {
                  doc.addField(fieldName, token);
               }
            }
            else {
               doc.addField(fieldName, Utils.UNKNOWN_PLACE);
            }
         }
      }
   }
}
