package org.werelate.search;

import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;

/**
 * Created by Dallan Quass
 * Date: May 22, 2008
 */
public class DateRewriteComponent extends BaseRewriteComponent
{
   protected void rewrite(TermRangeQuery q, BooleanClause.Occur occur, BooleanQuery out) {
//      logger.warning("CSRQ q="+q.toString()+" fld="+q.getField()+" lower="+q.getLowerVal()+" upper="+q.getUpperVal());
      String fieldName = q.getField();
      // turn 4-digit Date range queries into Year range queries
      if (fieldName.endsWith("Date") && q.getLowerTerm().length() == 4 && q.getUpperTerm().length() == 4) {
         out.add(new TermRangeQuery(fieldName.substring(0, fieldName.length() - 4)+"Year", q.getLowerTerm(), q.getUpperTerm(), true, true), occur);
      }
      else {
         out.add(q, occur);
      }
   }

   protected void rewrite(TermQuery q, BooleanClause.Occur occur, BooleanQuery out) {
//      logger.warning("TQ q="+q.toString()+" fld="+q.getTerm().field()+" value="+q.getTerm().text());
      String fieldName = q.getTerm().field();
      if (fieldName.endsWith("Date")) {
         String text = q.getTerm().text();
         if (text.length() == 4) {
            // turn a 4-digit Date query into a Year query
            out.add(new TermQuery(new Term(fieldName.substring(0, fieldName.length() - 4)+"Year", text)), occur);
         }
         else if (text.length() == 6) {
            // turn a 6-digit Date query into a Date range query on the month
            out.add(new TermRangeQuery(fieldName, text, text+"99", true, true), occur);
         }
         else {
            out.add(q, occur);
         }
      }
      else {
         out.add(q, occur);
      }
   }

   /////////////////////////////////////////////
   ///  SolrInfoMBean
   ////////////////////////////////////////////

   public String getDescription() {
     return "DateRewriteComponent";
   }

   public String getVersion() {
     return "DateRewriteComponent";
   }

   public String getSourceId() {
     return "DateRewriteComponent";
   }

   public String getSource() {
     return "DateRewriteComponent";
   }

}
