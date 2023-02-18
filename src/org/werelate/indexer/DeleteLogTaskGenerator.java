package org.werelate.indexer;

import org.werelate.util.HttpClientHelper;
import org.werelate.util.Utils;
import org.apache.commons.httpclient.methods.GetMethod;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.sql.SQLException;
import java.io.IOException;

import nu.xom.Element;
import nu.xom.Elements;
import nu.xom.ParsingException;

/**
 * Created by Dallan Quass
 * Date: Apr 18, 2008
 */
public class DeleteLogTaskGenerator extends BaseTaskGenerator
{
   public DeleteLogTaskGenerator(HttpClientHelper client, String hostname) {
      super(client, hostname);
   }

   public List<IndexTask> getTasks(CheckpointManager cm, int max) throws SQLException, IOException, ParsingException
   {
      List<IndexTask> tasks = new ArrayList<IndexTask>();

      Map<String,String> args = new HashMap<String,String>();
      String timestampPageid = cm.getCheckpoint();
      String[] fields = timestampPageid.split("/");
      args.put("timestamp", fields[0]);
      args.put("max", Integer.toString(max));
      GetMethod m = new GetMethod(Utils.getWikiAjaxUrl(hostname, "wfGetNewDeleteLogEntries", args));
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
            // ignore this check - the page may have been undeleted
//            // don't add the page id that we saw last time
//            if (fields.length < 2 || !row.getAttributeValue("page_id").equals(fields[1])) {
            tasks.add(new IndexTask(row.getAttributeValue("timestamp"),
                 row.getAttributeValue("page_id"), row.getAttributeValue("action").equalsIgnoreCase("delete") ? IndexTask.ACTION_DELETE : IndexTask.ACTION_UPDATE,
                                    0, null));
//            }
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
      cm.updateCheckpoint(it.getSequenceId()+"/"+it.getPageId());
   }
}
