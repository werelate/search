package org.werelate.search;

import org.apache.lucene.search.Query;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by Dallan Quass
 */
public class LoggerComponent extends SearchComponent {
   protected static Logger logger = Logger.getLogger("org.werelate.search");

   public void prepare(ResponseBuilder rb) throws IOException
   {
      if (rb.isDebug()) {
         Query qIn = rb.getQuery();
         logger.info("QUERY="+rb.getQueryString()+" FINA="+qIn.toString());
      }
   }

   public void process(ResponseBuilder rb) throws IOException
   {
      // nothing to do
   }

   /////////////////////////////////////////////
   ///  SolrInfoMBean
   ////////////////////////////////////////////

   public String getDescription() {
     return "LoggerComponent";
   }

   public String getVersion() {
     return "1.0";
   }

   public String getSourceId() {
     return "LoggerComponent";
   }

   public String getSource() {
     return "LoggerComponent";
   }

}
