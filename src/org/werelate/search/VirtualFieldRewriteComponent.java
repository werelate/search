package org.werelate.search;

import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.ConstantScorePrefixQuery;
import org.werelate.util.Utils;

import java.util.*;

/**
 * Created by Dallan Quass
 * Date: May 22, 2008
 */
public class VirtualFieldRewriteComponent extends BaseRewriteComponent
{
   public static final String[] PLACE_FIELDS = {Utils.FLD_PERSON_BIRTH_PLACE, Utils.FLD_PERSON_DEATH_PLACE, Utils.FLD_MARRIAGE_PLACE, Utils.FLD_OTHER_PLACE};
   public static final String[] PERSON_PLACE_FIELDS = {Utils.FLD_PERSON_BIRTH_PLACE, Utils.FLD_PERSON_DEATH_PLACE};
   public static final String[] SOURCE_ARTICLE_PLACE_FIELD = {Utils.FLD_OTHER_PLACE};

   public static final String[] SURNAME_FIELDS = {Utils.FLD_PERSON_SURNAME, Utils.FLD_HUSBAND_SURNAME, Utils.FLD_WIFE_SURNAME, Utils.FLD_OTHER_SURNAME};
   public static final String[] PERSON_SURNAME_FIELD = {Utils.FLD_PERSON_SURNAME};
   public static final String[] SOURCE_ARTICLE_SURNAME_FIELD = {Utils.FLD_OTHER_SURNAME};

   public static final String[] GIVENNAME_FIELDS = {Utils.FLD_PERSON_GIVENNAME, Utils.FLD_HUSBAND_GIVENNAME, Utils.FLD_WIFE_GIVENNAME, Utils.FLD_OTHER_GIVENNAME};
   public static final String[] PERSON_GIVENNAME_FIELD = {Utils.FLD_PERSON_GIVENNAME};

   public static final String IS_PERSON_NAMESPACE = "vf_is_person_namespace";
   public static final String IS_SOURCE_ARTICLE_NAMESPACE = "vf_is_source_article_namespace";

   protected void pre(BooleanQuery in, ResponseBuilder rb) {
      boolean isPersonNamespace = hasNamespaceFilter(rb.getFilters(), Utils.NS_PERSON_TEXT);
      boolean isSourceArticleNamespace = hasNamespaceFilter(rb.getFilters(), Utils.NS_SOURCE_TEXT) ||
                                         hasNamespaceFilter(rb.getFilters(), Utils.NS_ARTICLE_TEXT);
      Map<Object,Object> context = rb.req.getContext();
      context.put(IS_PERSON_NAMESPACE, isPersonNamespace);
      context.put(IS_SOURCE_ARTICLE_NAMESPACE, isSourceArticleNamespace);

   }

   private void addConstantScorePrefixQueries(String[] fieldNames, String prefix, BooleanClause.Occur occur, Query out) {
      for (String fieldName : fieldNames) {
         Utils.addQueryClause(out, new ConstantScorePrefixQuery(new Term(fieldName, prefix)), occur);
      }
   }

   protected void rewrite(ConstantScorePrefixQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("CSPQ q="+q.toString()+" fld="+q.getPrefix().field()+" value="+q.getPrefix().text());
      String fieldName = q.getPrefix().field();
      if (Utils.FLD_SURNAME.equals(fieldName)) {
         boolean isPersonNamespace = (Boolean)rb.req.getContext().get(IS_PERSON_NAMESPACE);
         boolean isSourceArticleNamespace = (Boolean)rb.req.getContext().get(IS_SOURCE_ARTICLE_NAMESPACE);
         if (isPersonNamespace || isSourceArticleNamespace) {
            addConstantScorePrefixQueries(isPersonNamespace ? PERSON_SURNAME_FIELD : SOURCE_ARTICLE_SURNAME_FIELD, q.getPrefix().text(), occur, out);
         }
         else if (occur == BooleanClause.Occur.MUST) {
            BooleanQuery bq = new BooleanQuery(true); // can use boolean query because we're not going to expand these terms in boost
            addConstantScorePrefixQueries(SURNAME_FIELDS, q.getPrefix().text(), BooleanClause.Occur.SHOULD, bq);
            out.add(bq, occur);
         }
         else {
            addConstantScorePrefixQueries(SURNAME_FIELDS, q.getPrefix().text(), occur, out);
         }
      }
      else if (Utils.FLD_GIVENNAME.equals(fieldName)) {
         boolean isPersonNamespace = (Boolean)rb.req.getContext().get(IS_PERSON_NAMESPACE);
         if (isPersonNamespace) {
            addConstantScorePrefixQueries(PERSON_GIVENNAME_FIELD, q.getPrefix().text(), occur, out);
         }
         else if (occur == BooleanClause.Occur.MUST) {
            BooleanQuery bq = new BooleanQuery(true);
            addConstantScorePrefixQueries(GIVENNAME_FIELDS, q.getPrefix().text(), BooleanClause.Occur.SHOULD, bq);
            out.add(bq, occur);
         }
         else {
            addConstantScorePrefixQueries(GIVENNAME_FIELDS, q.getPrefix().text(), occur, out);
         }
      }
      else {
         out.add(q, occur);
      }
   }

   private void addWildcardQueries(String[] fieldNames, String text, BooleanClause.Occur occur, Query out) {
      for (String fieldName : fieldNames) {
         Utils.addQueryClause(out, new WildcardQuery(new Term(fieldName, text)), occur);
      }
   }

   protected void rewrite(WildcardQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("WQ q="+q.toString()+" fld="+q.getTerm().field()+" value="+q.getTerm().text());
      String fieldName = q.getTerm().field();
      if (Utils.FLD_SURNAME.equals(fieldName)) {
         boolean isPersonNamespace = (Boolean)rb.req.getContext().get(IS_PERSON_NAMESPACE);
         boolean isSourceArticleNamespace = (Boolean)rb.req.getContext().get(IS_SOURCE_ARTICLE_NAMESPACE);
         if (isPersonNamespace || isSourceArticleNamespace) {
            addWildcardQueries(isPersonNamespace ? PERSON_SURNAME_FIELD : SOURCE_ARTICLE_SURNAME_FIELD, q.getTerm().text(), occur, out);
         }
         else if (occur == BooleanClause.Occur.MUST) {
            BooleanQuery bq = new BooleanQuery(true); // can use boolean query because we're not going to expand these terms in boost
            addWildcardQueries(SURNAME_FIELDS, q.getTerm().text(), BooleanClause.Occur.SHOULD, bq);
            out.add(bq, occur);
         }
         else {
            addWildcardQueries(SURNAME_FIELDS, q.getTerm().text(), occur, out);
         }
      }
      else if (Utils.FLD_GIVENNAME.equals(fieldName)) {
         boolean isPersonNamespace = (Boolean)rb.req.getContext().get(IS_PERSON_NAMESPACE);
         if (isPersonNamespace) {
            addWildcardQueries(PERSON_GIVENNAME_FIELD, q.getTerm().text(), occur, out);
         }
         else if (occur == BooleanClause.Occur.MUST) {
            BooleanQuery bq = new BooleanQuery(true);
            addWildcardQueries(GIVENNAME_FIELDS, q.getTerm().text(), BooleanClause.Occur.SHOULD, bq);
            out.add(bq, occur);
         }
         else {
            addWildcardQueries(GIVENNAME_FIELDS, q.getTerm().text(), occur, out);
         }
      }
      else {
         out.add(q, occur);
      }
   }

   private void addTermQueries(String[] fieldNames, String text, BooleanClause.Occur occur, Query out) {
      for (String fieldName : fieldNames) {
         Utils.addQueryClause(out, new TermQuery(new Term(fieldName, text)), occur);
      }
   }

   protected void rewrite(TermQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
//      logger.warning("TQ q="+q.toString()+" fld="+q.getTerm().field()+" value="+q.getTerm().text());
      String fieldName = q.getTerm().field();
      boolean isPersonNamespace = (Boolean)rb.req.getContext().get(IS_PERSON_NAMESPACE);
      boolean isSourceArticleNamespace = (Boolean)rb.req.getContext().get(IS_SOURCE_ARTICLE_NAMESPACE);
      Query temp;
      if (Utils.FLD_SURNAME.equals(fieldName)) {
         if (isPersonNamespace || isSourceArticleNamespace) { // single field
            addTermQueries(isPersonNamespace ? PERSON_SURNAME_FIELD : SOURCE_ARTICLE_SURNAME_FIELD, q.getTerm().text(), occur, out);
         }
         else { // add should-clauses for each field to dismax
            temp = new DisjunctionMaxQuery(0.0f);
            addTermQueries(SURNAME_FIELDS, q.getTerm().text(), BooleanClause.Occur.SHOULD, temp);
            out.add(temp, occur);
         }
      }
      else if (Utils.FLD_GIVENNAME.equals(fieldName)) {
         if (isPersonNamespace) {
            addTermQueries(PERSON_GIVENNAME_FIELD, q.getTerm().text(), occur, out);
         }
         else {
            temp = new DisjunctionMaxQuery(0.0f);
            addTermQueries(GIVENNAME_FIELDS, q.getTerm().text(), BooleanClause.Occur.SHOULD, temp);
            out.add(temp, occur);
         }
      }
      else if (Utils.FLD_PLACE.equals(fieldName)) {
         if (isSourceArticleNamespace) {
            addTermQueries(SOURCE_ARTICLE_PLACE_FIELD, q.getTerm().text(), occur, out);
         }
         else {
            temp = new DisjunctionMaxQuery(0.0f);
            addTermQueries(isPersonNamespace ? PERSON_PLACE_FIELDS : PLACE_FIELDS, q.getTerm().text(), BooleanClause.Occur.SHOULD, temp);
            out.add(temp, occur);
         }
      }
      else {
         out.add(q, occur);
      }
   }

   protected void rewrite(DisjunctionMaxQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      String[] placeFields;
      boolean isPersonNamespace = (Boolean)rb.req.getContext().get(IS_PERSON_NAMESPACE);
      boolean isSourceArticleNamespace = (Boolean)rb.req.getContext().get(IS_SOURCE_ARTICLE_NAMESPACE);
      if (isPersonNamespace) {
         placeFields = PERSON_PLACE_FIELDS;
      }
      else if (isSourceArticleNamespace) {
         placeFields = SOURCE_ARTICLE_PLACE_FIELD;
      }
      else {
         placeFields = PLACE_FIELDS;
      }

      // Disjunction queries can happen only on Place fields for now (from PlaceRewriteComponent matching multiple places)
      DisjunctionMaxQuery dq = new DisjunctionMaxQuery(0.0f);
      Iterator i = q.iterator();
      while (i.hasNext()) {
         TermQuery tq = (TermQuery)i.next();
         String fieldName = tq.getTerm().field();
         if (Utils.FLD_PLACE.equals(fieldName)) {
            addTermQueries(placeFields, tq.getTerm().text(), occur, dq);   // occur is ignored for dismax
         }
         else {
            Utils.addQueryClause(dq, tq, occur);   // occur is ignored for dismax
         }
      }
      out.add(dq, occur); // add dismax with same occur as orig
   }

   /////////////////////////////////////////////
   ///  SolrInfoMBean
   ////////////////////////////////////////////

   public String getDescription() {
     return "VirtualFieldRewriteComponent";
   }

   public String getVersion() {
     return "1.1";
   }

   public String getSourceId() {
     return "VirtualFieldRewriteComponent";
   }

   public String getSource() {
     return "VirtualFieldRewriteComponent";
   }
}
