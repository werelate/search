package org.werelate.search;

import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.*;
import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.werelate.util.Utils;

import java.util.logging.Logger;
import java.util.*;
import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: Jun 11, 2008
 */
public class QueryFixupComponent extends SearchComponent
{
   protected static Logger logger = Logger.getLogger("org.werelate.search");

   private static final int MAX_HIGHLIGHTED_RANGE = 20;

   private void addTermValues(String fieldName, String value, List<String> fieldValues)
   {
      if (fieldName.endsWith("Date")) {
         // split up dates
         if (value.length() >= 4) {
            fieldValues.add(value.substring(0, 4));
            if (value.length() >= 6) {
               String monthName = Utils.MONTH_NAMES[Integer.parseInt(value.substring(4,6))-1];
               fieldValues.add(monthName);
               if (monthName.length() > 3) {
                  fieldValues.add(monthName.substring(0,3));
               }
               if (value.length() == 8) {
                  fieldValues.add(value.substring(6, 8));
               }
            }
         }
      }
      else if (fieldName.endsWith("Place")) {
         // split up places
         for (String level : value.split(",")) {
            for (String word : level.split("\\s+")) {
               word = word.toLowerCase().trim();
               if (word.length() > 0) {
                  fieldValues.add(word);
               }
            }
         }
      }
      else if (Utils.FLD_USER.equals(fieldName) || Utils.FLD_SOURCE_SUBJECT.equals(fieldName) || Utils.FLD_SOURCE_AVAILABILITY.equals(fieldName)) {
         for (String word : value.split("\\s+")) {
            word = word.toLowerCase().trim();
            if (word.length() > 0) {
               fieldValues.add(word);
            }
         }
      }
      else {
         fieldValues.add(value);
      }
   }

   public void prepare(ResponseBuilder rb) throws IOException
   {
      SortSpec ss = rb.getSortSpec();
      Sort sort = ss.getSort();
      boolean isSort = (sort != null && sort.getSort() != null && sort.getSort().length > 0);

      Query qIn = rb.getQuery();
      if (!(qIn instanceof BooleanQuery)) {
         throw new RuntimeException("unexpected non-boolean query: "+ rb.getQueryString());
      }
      BooleanQuery in = (BooleanQuery)qIn;
      BooleanQuery out = new BooleanQuery(true);
      BooleanQuery hl = new BooleanQuery(true);
      boolean isSingleClause = in.getClauses().length == 1;
      String fieldName;
      List<String> highlightValues = new ArrayList<String>();

      for (BooleanClause bcIn : in.getClauses()) {
         qIn = bcIn.getQuery();
         BooleanClause.Occur occur = bcIn.getOccur();
         fieldName = "";
         highlightValues.clear();
         if (qIn instanceof TermRangeQuery) {
            fieldName = ((TermRangeQuery)qIn).getField();
            try {
               int lower = Integer.parseInt(((TermRangeQuery)qIn).getLowerTerm());
               int upper = Integer.parseInt(((TermRangeQuery)qIn).getUpperTerm());

               if (upper > lower && upper - lower <= MAX_HIGHLIGHTED_RANGE) {
                  for (int i = lower; i <= upper; i++) {
                     highlightValues.add(Integer.toString(i));
                  }
               }
            }
            catch (NumberFormatException e) {
               // ignore
            }
         }
         else if (qIn instanceof ConstantScorePrefixQuery) {
            fieldName = ((ConstantScorePrefixQuery)qIn).getPrefix().field();
         }
         else if (qIn instanceof PhraseQuery) {
            fieldName = ((PhraseQuery)qIn).getTerms()[0].field();
            for (Term t : ((PhraseQuery)qIn).getTerms()) {
               highlightValues.add(t.text());
            }
         }
         else if (qIn instanceof TermQuery) {
            fieldName = ((TermQuery)qIn).getTerm().field();
            addTermValues(fieldName, ((TermQuery)qIn).getTerm().text(), highlightValues);
         }
         else if (qIn instanceof WildcardQuery) {
            fieldName = ((WildcardQuery)qIn).getTerm().field();
         }

         // SHOULD -> MUST if sort
         if (isSort && BooleanClause.Occur.SHOULD.equals(occur)) {
            occur = BooleanClause.Occur.MUST;
         }
//         else if (!isSort && isSingleClause && BooleanClause.Occur.MUST.equals(occur)) {
//            if (Utils.FLD_KEYWORDS.equals(fieldName) || fieldName.endsWith("Date") || fieldName.endsWith("Year") ||
//                fieldName.endsWith("Place") || fieldName.endsWith("Surname") || fieldName.endsWith("Givenname")) {
//               // MUST -> SHOULD if not sort and single clause - boosting will add additional clauses for these fields
//               occur = BooleanClause.Occur.SHOULD;
//            }
//         }
         out.add(qIn, occur);

         // add to highlight query
         if (fieldName.length() > 0 && highlightValues.size() > 0) {
            String[] hlFields = Utils.HIGHLIGHT_FIELDS.get(fieldName);
            if (hlFields != null) {
               for (String hlField : hlFields) {
                  for (String fieldValue : highlightValues) {
                     hl.add(new TermQuery(new Term(hlField, fieldValue)), BooleanClause.Occur.SHOULD);
                  }
               }
            }
         }
      }
      rb.setQuery(out);
      rb.setHighlightQuery(hl);
   }

   public void process(ResponseBuilder rb) throws IOException
   {
      // nothing to do
   }

   /////////////////////////////////////////////
   ///  SolrInfoMBean
   ////////////////////////////////////////////

   public String getDescription() {
     return "QueryFixupComponent";
   }

   public String getVersion() {
     return "1.1";
   }

   public String getSourceId() {
     return "QueryFixupComponent";
   }

   public String getSource() {
     return "QueryFixupComponent";
   }
}
