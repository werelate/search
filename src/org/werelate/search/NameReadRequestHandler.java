package org.werelate.search;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class NameReadRequestHandler extends RequestHandlerBase {

   public NameReadRequestHandler() {
      super();
   }

   @Override
   @SuppressWarnings("unchecked")
   public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception, ParseException, InstantiationException, IllegalAccessException
   {
      // get query parameters
      SolrParams params = req.getParams();
      String name = params.get("name");
      String type = params.get("type");

      NameReader nameReader = NameReader.getInstance();
      NameReader.NameInfo nameInfo = nameReader.getNameInfo(name, type != null && type.startsWith("s"));
      if (nameInfo.namePiece != null) {
         rsp.add("name", nameInfo.namePiece);
      }
      if (nameInfo.confirmedVariants != null) {
         rsp.add("confirmedVariants", nameInfo.confirmedVariants);
      }
      if (nameInfo.computerVariants != null) {
         rsp.add("computerVariants", nameInfo.computerVariants);
      }
      if (nameInfo.candidateVariants != null) {
         rsp.add("candidateVariants", nameInfo.candidateVariants);
      }
      if (nameInfo.soundexExamples != null) {
         rsp.add("soundexExamples", nameInfo.soundexExamples);
      }
      if (nameInfo.basename != null) {
         rsp.add("basename", nameInfo.basename);
      }
      if (nameInfo.prefixedNames != null) {
         rsp.add("prefixedNames", nameInfo.prefixedNames);
      }

   }

   //////////////////////// SolrInfoMBeans methods //////////////////////

   public String getDescription() {
      return "Name read request handler";
   }

   public String getVersion() {
      return "$Revision$";
   }

   public String getSourceId() {
      return "$Id: NameReadRequestHandler.java $";
   }

   public String getSource() {
      return "$URL:  $";
   }
}
