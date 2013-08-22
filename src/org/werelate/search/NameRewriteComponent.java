package org.werelate.search;

import org.apache.lucene.search.*;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.search.ConstantScorePrefixQuery;
import org.werelate.util.Utils;

import java.util.List;

/**
 * Created by Dallan Quass
 * Date: May 22, 2008
 */
public class NameRewriteComponent extends BaseRewriteComponent
{
   protected void rewrite(PhraseQuery q, BooleanClause.Occur occur, BooleanQuery out) {
//      logger.warning("PQ q="+q.toString()+"term cnt="+q.getTerms().length+" fld[0]="+q.getTerms()[0].field()+" value[0]="+q.getTerms()[0].text());
      // turn phrase query for surname/givenname/placename field into a boolean query
      String fieldName = q.getTerms()[0].field();
      if (fieldName.endsWith("Surname") || fieldName.endsWith("Givenname") || fieldName.equals("PlaceName")) {
         for (Term t : q.getTerms()) {
            out.add(new TermQuery(t), occur);
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
     return "NameRewriteComponent";
   }

   public String getVersion() {
     return "1.1";
   }

   public String getSourceId() {
     return "NameRewriteComponent";
   }

   public String getSource() {
     return "NameRewriteComponent";
   }

}
