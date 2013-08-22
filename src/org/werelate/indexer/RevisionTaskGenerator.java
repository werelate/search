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
public class RevisionTaskGenerator extends BaseTaskGenerator
{
   public RevisionTaskGenerator(HttpClientHelper client, String hostname) {
      super(client, hostname);
   }

   public List<IndexTask> getTasks(CheckpointManager cm, int max) throws SQLException, IOException, ParsingException
   {
      List<IndexTask> tasks = new ArrayList<IndexTask>();

      Map<String,String> args = new HashMap<String,String>();
      args.put("rev_id", cm.getCheckpoint());
      args.put("max", Integer.toString(max));
      GetMethod m = new GetMethod(Utils.getWikiAjaxUrl(hostname, "wfGetNewRevisions", args));
      logger.info("Request " + m.getURI());
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
            tasks.add(new IndexTask(row.getAttributeValue("rev_id"), row.getAttributeValue("page_id"), IndexTask.ACTION_UPDATE,
                                    Integer.parseInt(row.getAttributeValue("rev_id")), row.getAttributeValue("rev_timestamp")));
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
      cm.updateCheckpoint(it.getSequenceId(), it.getRevTimestamp());
   }
}
