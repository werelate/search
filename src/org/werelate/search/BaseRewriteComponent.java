package org.werelate.search;

import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.ConstantScorePrefixQuery;
import org.apache.lucene.search.*;
import org.werelate.util.Utils;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.List;

/**
 * Created by Dallan Quass
 * Date: May 21, 2008
 */
public abstract class BaseRewriteComponent extends SearchComponent
{
   protected static Logger logger = Logger.getLogger("org.werelate.search");

   public void prepare(ResponseBuilder rb) throws IOException
   {
      Query qIn = rb.getQuery();
      if (!(qIn instanceof BooleanQuery)) {
         logger.severe("unexpected non-boolean query: "+ rb.getQueryString());
         return; // we only handle boolean queries
      }
      BooleanQuery in = (BooleanQuery)qIn;
      pre(in, rb);

      BooleanQuery out = new BooleanQuery(true);
      for (BooleanClause bcIn : in.getClauses()) {
         qIn = bcIn.getQuery();
         BooleanClause.Occur occur = bcIn.getOccur();

         if (qIn instanceof TermQuery) {
            rewrite((TermQuery)qIn, occur, out, rb);
         }
         else if (qIn instanceof PhraseQuery) {
            rewrite((PhraseQuery)qIn, occur, out, rb);
         }
         else if (qIn instanceof DisjunctionMaxQuery) {
            rewrite((DisjunctionMaxQuery)qIn, occur, out, rb);
         }
         else if (qIn instanceof ConstantScorePrefixQuery) {
            rewrite((ConstantScorePrefixQuery)qIn, occur, out, rb);
         }
         else if (qIn instanceof WildcardQuery) {
            rewrite((WildcardQuery)qIn, occur, out, rb);
         }
         else if (qIn instanceof TermRangeQuery) {
            rewrite((TermRangeQuery)qIn, occur, out, rb);
         }
         else if (qIn instanceof BooleanQuery) {
            rewrite((BooleanQuery)qIn, occur, out, rb);
         }
         else {
            logger.info("Unknown query="+qIn.getClass().getName()+": "+qIn.toString());
            out.add(qIn, occur);
         }
      }

      post(out, rb);
      rb.setQuery(out);
   }

   protected void pre(BooleanQuery in, ResponseBuilder rb) {
      // nothing to do
   }

   protected void rewrite(TermQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      rewrite(q, occur, out);
   }

   protected void rewrite(TermQuery q, BooleanClause.Occur occur, BooleanQuery out) {
      out.add(q, occur);
   }

   protected void rewrite(PhraseQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      rewrite(q, occur, out);
   }

   protected void rewrite(PhraseQuery q, BooleanClause.Occur occur, BooleanQuery out) {
      out.add(q, occur);
   }

   protected void rewrite(DisjunctionMaxQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      rewrite(q, occur, out);
   }

   protected void rewrite(DisjunctionMaxQuery q, BooleanClause.Occur occur, BooleanQuery out) {
      out.add(q, occur);
   }

   protected void rewrite(ConstantScorePrefixQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      rewrite(q, occur, out);
   }

   protected void rewrite(ConstantScorePrefixQuery q, BooleanClause.Occur occur, BooleanQuery out) {
      out.add(q, occur);
   }

   protected void rewrite(WildcardQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      rewrite(q, occur, out);
   }

   protected void rewrite(WildcardQuery q, BooleanClause.Occur occur, BooleanQuery out) {
      out.add(q, occur);
   }

   protected void rewrite(TermRangeQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      rewrite(q, occur, out);
   }

   protected void rewrite(TermRangeQuery q, BooleanClause.Occur occur, BooleanQuery out) {
      out.add(q, occur);
   }

   protected void rewrite(BooleanQuery q, BooleanClause.Occur occur, BooleanQuery out, ResponseBuilder rb) {
      rewrite(q, occur, out);
   }

   protected void rewrite(BooleanQuery q, BooleanClause.Occur occur, BooleanQuery out) {
      out.add(q, occur);
   }

   protected void post(BooleanQuery out, ResponseBuilder rb) {
      // nothing to do
   }

   public void process(ResponseBuilder rb) throws IOException
   {
      // nothing to do
   }

   protected boolean hasNamespaceFilter(List<Query> filterQueries, String nsText) {
      if (filterQueries != null) {
         for (Query q : filterQueries) {
            if (q instanceof TermQuery) {
               if (Utils.FLD_NAMESPACE.equals(((TermQuery)q).getTerm().field()) &&
                   nsText.equals(((TermQuery)q).getTerm().text())) {
                  return true;
               }
            }
         }
      }
      return false;
   }


}
