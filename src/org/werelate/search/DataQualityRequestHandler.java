/*
 * Copyright 2012 Foundation for On-Line Genealogy, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.werelate.search;

import org.apache.lucene.queryParser.ParseException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import nu.xom.Builder;
import nu.xom.Element;
import java.util.logging.Logger;

import org.werelate.dq.FamilyDQAnalysis;
import org.werelate.dq.PersonDQAnalysis;
import org.werelate.util.Utils;

public class DataQualityRequestHandler extends RequestHandlerBase {
   private static final Logger logger = Logger.getLogger("org.werelate.search");

   public DataQualityRequestHandler() {
      super();
   }

   @Override
   @SuppressWarnings("unchecked")
   public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception, ParseException, InstantiationException, IllegalAccessException
   {
      // get common query parameters
      SolrParams params = req.getParams();
      String data = params.get("data");
      String namespace = params.get("ns");

      Element root = Utils.parseText(new Builder(), data, true).getRootElement();
      String[][] issues = new String[1000][4];

      // Get issues for a person page
      if (namespace.equals("Person")) {
         String personTitle = params.get("pTitle");
         PersonDQAnalysis personDQAnalysis = new PersonDQAnalysis(root, personTitle);
         issues = personDQAnalysis.getIssues();
         addIssuesToResponse(issues, rsp);
      }

      // Get issues for a family page or for a child in relation to the parents' family page
      if (namespace.equals("Family")) {
         String familyTitle = params.get("ftitle");
         String childTitle = params.get("ctitle");
         FamilyDQAnalysis familyDQAnalysis = new FamilyDQAnalysis(root, familyTitle, childTitle);
         issues = familyDQAnalysis.getIssues();
         addIssuesToResponse(issues, rsp);
      }
   }

   private void addIssuesToResponse(String[][] issues, SolrQueryResponse rsp) {
      for (int i=0; issues[i][0] != null; i++) {
         rsp.add("issue " + (i+1), issues[i]);
      }
   }


   //////////////////////// SolrInfoMBeans methods //////////////////////

   public String getDescription() {
      return "Data Quality request handler";
   }

   public String getVersion() {
      return "$Revision$";
   }

   public String getSourceId() {
      return "$Id: DataQualityRequestHandler.java $";
   }

   public String getSource() {
      return "$URL:  $";
   }
}
