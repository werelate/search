package org.werelate.wiki;

/**
 * Created by Dallan Quass
 * Date: May 8, 2008
 */
public class IndexInstruction
{
   public static final int FILTER_NO_FILTER = 0;
   public static final int FILTER_REMOVE_PRE_BAR = 1;
   public static final int FILTER_REMOVE_POST_BAR = 2;
   public static final int FILTER_REMOVE_LINK = 3;

   private String fieldName;
   private String xPath;
   private String defaultValue;
   private int filterAction;

   public IndexInstruction(String fieldName, String xPath) {
      this(fieldName, xPath, null, FILTER_NO_FILTER);
   }

   public IndexInstruction(String fieldName, String xPath, String defaultValue) {
      this(fieldName, xPath, defaultValue, FILTER_NO_FILTER);
   }

   public IndexInstruction(String fieldName, String xPath, String defaultValue, int filterAction) {
      this.fieldName = fieldName;
      this.xPath = xPath;
      this.defaultValue = defaultValue;
      this.filterAction = filterAction;
   }

   public String getFieldName()
   {
      return fieldName;
   }

   public String getXPath()
   {
      return xPath;
   }

   public String getDefaultValue()
   {
      return defaultValue;
   }

   public int getFilterAction()
   {
      return filterAction;
   }
}
