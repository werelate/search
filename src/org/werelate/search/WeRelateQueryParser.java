package org.werelate.search;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Version;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.apache.solr.search.ConstantScorePrefixQuery;
import org.werelate.util.Utils;

import java.util.logging.Logger;

/**
 * Created by Dallan Quass
 * Date: May 21, 2008
 */
public class WeRelateQueryParser extends QueryParser {
   public static final int MAX_YEAR_RANGE = 10;
   public static final int MIN_PREFIX_LENGTH = 3;

   protected final IndexSchema schema;

   private static Logger logger = Logger.getLogger("org.werelate.search");

   /**
    * Constructs a SolrQueryParser using the schema to understand the
    * formats and datatypes of each field.  Only the defaultSearchField
    * will be used from the IndexSchema (unless overridden),
    * &lt;solrQueryParser&gt; will not be used.
    *
    * @param schema Used for default search field name if defaultField is null and field information is used for analysis
    * @param defaultField default field used for unspecified search terms.  if null, the schema default field is used
    * @see IndexSchema#getSolrQueryParser(String defaultField)
    */
   public WeRelateQueryParser(IndexSchema schema, String defaultField) {
      super(Version.LUCENE_23, defaultField == null ? schema.getDefaultSearchFieldName() : defaultField, schema.getQueryAnalyzer());
      this.schema = schema;
//      setLowercaseExpandedTerms(false);
   }

   public WeRelateQueryParser(QParser parser, String defaultField) {
      super(Version.LUCENE_23, defaultField, parser.getReq().getSchema().getQueryAnalyzer());
      this.schema = parser.getReq().getSchema();
//      setLowercaseExpandedTerms(false);
   }

   protected Query getRangeQuery(String field, String part1, String part2, boolean inclusive) throws ParseException {
      String fieldType = schema.getFieldType(field).getTypeName();
      if ("year".equals(fieldType) || "date".equals(fieldType)) {
         try {
            int p1 = Integer.parseInt(part1);
            int p2 = Integer.parseInt(part2);
            if (p2 < p1 || p2 - p1 > MAX_YEAR_RANGE) {
               throw new ParseException("Range must be 10 years or less");
            }
         }
         catch (NumberFormatException e) {
            throw new ParseException("Dates ranges must contain 4-digit years only");
         }
      }
      else {
         throw new ParseException("[ and ] characters not allowed in query except for year ranges");
      }
      Query q = super.getRangeQuery(field, part1, part2, inclusive);
//     logger.warning("getRangeQuery class="+q.getClass()+" q="+q.toString()+" ft="+ft.getTypeName()+" field="+field+" part1="+part1+" part2="+part2+" inclusive="+inclusive);
      return q;
   }

   protected Query getPrefixQuery(String field, String termStr) throws ParseException {
      if (termStr.length() < MIN_PREFIX_LENGTH) {
         throw new ParseException("Need at least 3 letters");
      }
      String fieldType = schema.getFieldType(field).getTypeName();
      if ("year".equals(fieldType) || "date".equals(fieldType)) {
         throw new ParseException("* not allowed in dates; try a year..year range instead");
      }
      if ("place".equals(fieldType) || "placelocatedin".equals(fieldType)) {
         throw new ParseException("* not allowed in places, except in place names in place-namespace search");
      }

      // assume that term text should be romanized and lowercased
      // TODO possibly treat names specially by tokenizing them
      Term t = new Term(field, Utils.romanize(termStr).toLowerCase());
      Query q = new ConstantScorePrefixQuery(t);
//      logger.warning("getPrefixQuery q="+q.toString()+" field="+field+" termStr="+termStr);
      return q;
   }

   private int numCharsInWildcard(String s) {
      int numChars = 0;
      for (int i = 0; i < s.length(); i++) {
         char c = s.charAt(i);
         if (c != '?' && c != '*') {
            numChars++;
         }
      }
      return numChars;
   }

   protected Query getWildcardQuery(String field, String termStr) throws ParseException {
      if (numCharsInWildcard(termStr) < MIN_PREFIX_LENGTH) {
         throw new ParseException("Need at least 3 non-wildcard characters");
      }
      // TODO possibly treat names specially by tokenizing them
      return super.getWildcardQuery(field, Utils.romanize(termStr).toLowerCase());
   }

   protected Query getFuzzyQuery(String field, String termStr, float v) throws ParseException {
      throw new ParseException("~ character not allowed in query");
   }

}
