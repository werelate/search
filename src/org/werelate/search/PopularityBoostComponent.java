package org.werelate.search;

import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.FunctionQParser;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.werelate.util.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: dallan
 */
public class PopularityBoostComponent extends SearchComponent {
   public static final String POPULARITY_BOOST = "div(Popularity,1024)";
   protected static Logger logger = Logger.getLogger("org.werelate.search");

   @Override
   public void prepare(ResponseBuilder rb) throws IOException {
      if (Utils.isEmpty(rb.req.getParams().get("exact"))) {
         Query qIn = rb.getQuery();
         BooleanQuery out = new BooleanQuery(true);
         out.add(new BooleanClause(qIn, BooleanClause.Occur.MUST));
         try {
            Query bq = QParser.getParser(POPULARITY_BOOST, "func", rb.req).getQuery();
            out.add(new BooleanClause(bq, BooleanClause.Occur.SHOULD));
         } catch (ParseException e) {
            throw new RuntimeException(e);
         }
         rb.setQuery(out);
      }
   }

   @Override
   public void process(ResponseBuilder rb) throws IOException {
      // nothing to do
   }

   @Override
   public String getDescription() {
      return "popularity boost";
   }

   @Override
   public String getSourceId() {
      return "source id";
   }

   @Override
   public String getSource() {
      return "source";
   }

   @Override
   public String getVersion() {
      return "1.0";
   }
}
