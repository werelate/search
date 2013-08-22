package org.werelate.indexer;

/**
 * Created by Dallan Quass
 * Date: Apr 18, 2008
 */
public class IndexTask
{
   public static final int ACTION_UPDATE = 0;
   public static final int ACTION_DELETE = 1;

   private String sequenceId;
   private String pageId;
   private int action;
   private int revId;
   private String revTimestamp;

   public IndexTask() {
      this(null, null, 0, 0, null);
   }

   public IndexTask(String sequenceId, String pageId, int action, int revId, String revTimestamp) {
      this.sequenceId = sequenceId;
      this.pageId = pageId;
      this.action = action;
      this.revId = revId;
      this.revTimestamp = revTimestamp;
   }

   public String getSequenceId()
   {
      return sequenceId;
   }

   public void setSequenceId(String sequenceId)
   {
      this.sequenceId = sequenceId;
   }

   public String getPageId()
   {
      return pageId;
   }

   public void setPageId(String pageId)
   {
      this.pageId = pageId;
   }

   public int getRevId()
   {
      return revId;
   }

   public void setRevId(int revId)
   {
      this.revId = revId;
   }

   public String getRevTimestamp()
   {
      return revTimestamp;
   }

   public void setRevTimestamp(String revTimestamp)
   {
      this.revTimestamp = revTimestamp;
   }

   public int getAction()
   {
      return action;
   }

   public void setAction(int action)
   {
      this.action = action;
   }
}
