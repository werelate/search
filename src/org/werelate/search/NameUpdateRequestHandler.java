package org.werelate.search;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.Collection;

public class NameUpdateRequestHandler extends RequestHandlerBase {

   public NameUpdateRequestHandler() {
      super();
   }

   @Override
   @SuppressWarnings("unchecked")
   public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception
   {
      // get query parameters
      SolrParams params = req.getParams();
      String name = params.get("name");
      String type = params.get("type");
      String userName = params.get("userName");
      String comment = params.get("comment");
      String[] adds = (params.get("adds") != null && params.get("adds").length() > 0) ? params.get("adds").split(" ") : new String[0];
      String[] deletes = (params.get("deletes") != null && params.get("deletes").length() > 0) ? params.get("deletes").split(" ") : new String[0];
      boolean isAdmin = "true".equalsIgnoreCase(params.get("isAdmin"));

      NameUpdater nameUpdater = NameUpdater.getInstance();
      Collection<String> soundexNames = nameUpdater.update(name, type != null && type.startsWith("s"), userName, adds, deletes, comment, isAdmin);
      if (soundexNames != null) {
         rsp.add("soundex", soundexNames);
      }
   }

   //////////////////////// SolrInfoMBeans methods //////////////////////

   public String getDescription() {
      return "Name update request handler";
   }

   public String getVersion() {
      return "$Revision$";
   }

   public String getSourceId() {
      return "$Id: NameUpdateRequestHandler.java $";
   }

   public String getSource() {
      return "$URL:  $";
   }
}
