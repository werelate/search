package org.werelate.search;

import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;

import java.util.logging.Logger;

/**
 * Created by Dallan Quass
 * Date: May 21, 2008
 */
public class WeRelateQParserPlugin extends QParserPlugin
{
   public static String NAME = "werelate";
   private static Logger logger = Logger.getLogger("org.werelate.search");

   public void init(NamedList args) {
      // nothing to do
   }

   public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      return new WeRelateQParser(qstr, localParams, params, req);
   }
}

class WeRelateQParser extends QParser {
   WeRelateQueryParser parser;

   private static Logger logger = Logger.getLogger("org.werelate.search");

   public WeRelateQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
   }


   public Query parse() throws ParseException {
      String qstr = getString();

      String defaultField = getParam(CommonParams.DF);
      if (defaultField==null) {
         defaultField = getReq().getSchema().getDefaultSearchFieldName();
      }
      parser = new WeRelateQueryParser(this, defaultField);
      parser.setEnablePositionIncrements(true);

      // these could either be checked & set here, or in the WeRelateQueryParser constructor
      String opParam = getParam(QueryParsing.OP);
      if (opParam != null) {
         parser.setDefaultOperator("AND".equals(opParam) ? QueryParser.Operator.AND : QueryParser.Operator.OR);
      } else {
         String sortStr = getParams().get(CommonParams.SORT);
         parser.setDefaultOperator(sortStr == null ? QueryParser.Operator.OR : QueryParser.Operator.AND);
      }

      Query q = parser.parse(qstr);
      // force into a boolean query
      if (!(q instanceof BooleanQuery)) {
         BooleanQuery bq = new BooleanQuery(true);
         bq.add(q, BooleanClause.Occur.MUST);
         q = bq;
      }
      return q;
   }
}
