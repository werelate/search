package org.werelate.indexer;

import org.werelate.util.HttpClientHelper;
import org.werelate.util.Utils;
import org.apache.commons.httpclient.methods.GetMethod;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.sql.SQLException;
import java.io.IOException;

import nu.xom.ParsingException;
import nu.xom.Element;
import nu.xom.Elements;

/**
 * Created by Dallan Quass
 * Date: Apr 18, 2008
 */
public class IndexRequestTaskGenerator extends BaseTaskGenerator
{
   public IndexRequestTaskGenerator(HttpClientHelper client, String hostname) {
      super(client, hostname);
   }

   public List<IndexTask> getTasks(CheckpointManager cm, int max) throws SQLException, IOException, ParsingException
   {
      List<IndexTask> tasks = new ArrayList<IndexTask>();

      Map<String,String> args = new HashMap<String,String>();
      args.put("ir_id", cm.getCheckpoint());
      args.put("max", Integer.toString(max));
      GetMethod m = new GetMethod(Utils.getWikiAjaxUrl(hostname, "wfGetNewIndexRequests", args));
      try
      {
         client.executeHttpMethod(m);
         Element root = client.parseText(HttpClientHelper.getResponse(m)).getRootElement();
         if (Integer.parseInt(root.getAttributeValue("status")) != HttpClientHelper.STATUS_OK) {
            throw new RuntimeException("Unexpected status="+root.getAttributeValue("status"));
         }
         Elements rows = root.getChildElements("row");
         for (int i=0; i < rows.size(); i++)
         {
            Element row = rows.get(i);
            tasks.add(new IndexTask(row.getAttributeValue("ir_id"), row.getAttributeValue("page_id"), IndexTask.ACTION_UPDATE, 0, null));
         }
      }
      finally
      {
         m.releaseConnection();
      }

      return tasks;
   }

   public void updateCheckpoint(IndexTask it, CheckpointManager cm)
   {
      cm.updateCheckpoint(it.getSequenceId());
   }
}
