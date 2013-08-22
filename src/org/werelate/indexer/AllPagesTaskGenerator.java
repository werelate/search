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
public class AllPagesTaskGenerator extends BaseTaskGenerator
{
   private boolean rollOver;
   private String[] namespaces;
   private boolean atEnd;

   public AllPagesTaskGenerator(HttpClientHelper client, String hostname) {
      this(client, hostname, true, null);
   }

   public AllPagesTaskGenerator(HttpClientHelper client, String hostname, boolean rollOver, String[] namespaces) {
      super(client, hostname);
      this.rollOver = rollOver;
      this.namespaces = namespaces;
      this.atEnd = false;
   }

   public List<IndexTask> getTasks(CheckpointManager cm, int max) throws SQLException, IOException, ParsingException
   {
      List<IndexTask> tasks = new ArrayList<IndexTask>();

      Map<String,String> args = new HashMap<String,String>();
      args.put("page_id", cm.getCheckpoint());
      args.put("max", Integer.toString(max));
      if (namespaces != null && namespaces.length > 0) {
         args.put("namespaces", Utils.join(",", namespaces));
      }
      GetMethod m = new GetMethod(Utils.getWikiAjaxUrl(hostname, "wfGetAllPageIds", args));
      try
      {
         client.executeHttpMethod(m);
         Element root = client.parseText(HttpClientHelper.getResponse(m)).getRootElement();
         if (Integer.parseInt(root.getAttributeValue("status")) != HttpClientHelper.STATUS_OK) {
            throw new RuntimeException("Unexpected status="+root.getAttributeValue("status"));
         }
         Elements rows = root.getChildElements("row");

         // are we at the end? if rolling over,, don't bother indexing these because they will have just been indexed; instead, start over
         if (rows.size() < max && !rollOver) {
            atEnd = true;
         }
         if (rows.size() < max && rollOver) {
            logger.warning("Resetting all-pages checkpoint to 0");
            cm.updateCheckpoint("0");
         }
         else {
            for (int i=0; i < rows.size(); i++)
            {
               Element row = rows.get(i);
               tasks.add(new IndexTask(row.getAttributeValue("page_id"), row.getAttributeValue("page_id"), IndexTask.ACTION_UPDATE, 0, null));
            }
         }
//         logger.info("allpages requested max="+max+" got="+rows.size());
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

   public boolean isAtEnd() {
      return atEnd;
   }
}
