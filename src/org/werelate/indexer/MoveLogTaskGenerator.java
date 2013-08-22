package org.werelate.indexer;

import net.spy.memcached.MemcachedClient;
import nu.xom.Element;
import nu.xom.Elements;
import nu.xom.ParsingException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.werelate.util.HttpClientHelper;
import org.werelate.util.Utils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 * User: dallan
 */
public class MoveLogTaskGenerator extends BaseTaskGenerator
{
   private MemcachedClient memcache;

   public MoveLogTaskGenerator(HttpClientHelper client, String hostname, MemcachedClient memcache) {
      super(client, hostname);
      this.memcache = memcache;
   }

   public List<IndexTask> getTasks(CheckpointManager cm, int max) throws SQLException, IOException, ParsingException {
      List<IndexTask> tasks = new ArrayList<IndexTask>();

      Map<String,String> args = new HashMap<String,String>();
      String timestampPageid = cm.getCheckpoint();
      String[] fields = timestampPageid.split("/");
      args.put("timestamp", fields[0]);
      args.put("max", Integer.toString(max));
      GetMethod m = new GetMethod(Utils.getWikiAjaxUrl(hostname, "wfGetNewMoveLogEntries", args));
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
            // don't add this row if we saw it the last time
            if (!(fields.length == 2 &&
                  row.getAttributeValue("timestamp").equals(fields[0]) &&
                  row.getAttributeValue("target_id").equals(fields[1]))) {
               // remove source
               if (!row.getAttributeValue("source_id").equals("0")) {
                  String title = row.getAttributeValue("source_title");
                  if (title.startsWith("Place:")) {
                     memcache.delete(Utils.getMemcacheKey(PlaceStandardizer.MC_PREFIX, title));
                  }
                  tasks.add(new IndexTask(row.getAttributeValue("timestamp"), row.getAttributeValue("source_id"), IndexTask.ACTION_DELETE, 0, null));
               }
               // add target
               if (!row.getAttributeValue("target_id").equals("0")) {
                  tasks.add(new IndexTask(row.getAttributeValue("timestamp"), row.getAttributeValue("target_id"), IndexTask.ACTION_UPDATE, 0, null));
               }
            }
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
