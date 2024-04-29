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

import org.werelate.util.EventDate;

public class EventDateRequestHandler extends RequestHandlerBase {
   private static final Logger logger = Logger.getLogger("org.werelate.search");

   public EventDateRequestHandler() {
      super();
   }

   @Override
   @SuppressWarnings("unchecked")
   public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception, ParseException, InstantiationException, IllegalAccessException
   {
      // get common query parameters
      SolrParams params = req.getParams();
      String edit = params.get("edit");
      String date = params.get("date");
      String eventType = params.get("type");
      EventDate eventDate;

      // Parse the date and return variables. Return variables that require editing the date only if requested to.
      if (eventType == null) {
         eventDate = new EventDate(date);
      }
      else {
         eventDate = new EventDate(date, eventType);
      }
      if (edit != null && edit.equals("yes")) {
         rsp.add("formatedDate", eventDate.getFormatedDate());
         rsp.add("errorMessage", eventDate.getErrorMessage());
         rsp.add("significantReformat", eventDate.getSignificantReformat());
         rsp.add("yearRange", eventDate.getYearRange());
         rsp.add("parsedDate", eventDate.getParsedDate());
      }
      rsp.add("yearOnly", eventDate.getYearOnly());
      rsp.add("effectiveYear", eventDate.getEffectiveYear());
      rsp.add("effectiveStartYear", eventDate.getEffectiveStartYear());
      rsp.add("dateSortKey", eventDate.getDateSortKey());
      rsp.add("dateStringKey", eventDate.getDateStringKey());
      rsp.add("isoDate", eventDate.getIsoDate());
   }


   //////////////////////// SolrInfoMBeans methods //////////////////////

   public String getDescription() {
      return "EventDate request handler";
   }

   public String getVersion() {
      return "$Revision$";
   }

   public String getSourceId() {
      return "$Id: EventDateRequestHandler.java $";
   }

   public String getSource() {
      return "$URL:  $";
   }
}
