package org.werelate.wiki;

import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

import nu.xom.Document;
import nu.xom.Nodes;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class SourcePageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_SURNAME, "source/surname"),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "source/place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_SURNAME_STORED, "source/surname"),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "source/place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_AUTHOR, "source/author", null, IndexInstruction.FILTER_REMOVE_LINK),
      new IndexInstruction(Utils.FLD_SOURCE_TITLE_STORED, "source/source_title"),
      new IndexInstruction(Utils.FLD_SOURCE_SUBJECT, "source/source_category"),
      new IndexInstruction(Utils.FLD_SOURCE_SUBJECT, "source/subject"),
      new IndexInstruction(Utils.FLD_SOURCE_AVAILABILITY, "source/repository/@availability"),
      new IndexInstruction(Utils.FLD_SOURCE_SUB_SUBJECT, "source/ethnicity"),
      new IndexInstruction(Utils.FLD_SOURCE_SUB_SUBJECT, "source/religion"),
      new IndexInstruction(Utils.FLD_SOURCE_SUB_SUBJECT, "source/occupation"),
      new IndexInstruction(Utils.FLD_FROM_YEAR, "source/from_year"),
      new IndexInstruction(Utils.FLD_TO_YEAR, "source/to_year"),
   };

   public SourcePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "source";
   }

   protected String getTitleIndex(String title, Document xml) {
      StringBuilder buf = new StringBuilder();
      if (!Utils.isEmpty(title)) {
         buf.append(title);
      }
      if (xml != null) {
         Nodes nodes = xml.query("source/source_title");
         for (int i = 0; i < nodes.size(); i++) {
            buf.append(" ");
            buf.append(nodes.get(i).getValue());
         }
      }
      return buf.toString();
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }
}
