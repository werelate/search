package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

import nu.xom.Document;
import nu.xom.Nodes;
import nu.xom.Element;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class ImagePageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_OTHER_SURNAME, "image_data/person/@surname"),
      new IndexInstruction(Utils.FLD_OTHER_GIVENNAME, "image_data/person/@given"),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "image_data/place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_PLACE_STORED, "image_data/place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
   };

   public ImagePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "image_data";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   // index family name pieces; store fullname(s)
   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      if (xml != null) {
         Nodes nodes = xml.query("image_data/family/@title");
         for (int i = 0; i < nodes.size(); i++) {
            String familyTitle = removeIndexNumber(nodes.get(i).getValue());
            doc.addField(Utils.FLD_FULLNAME_STORED, familyTitle);
            String[][] namePieces = getFamilyNamePieces(familyTitle);
            for (int j = 0; j < 2; j++) {
               for (int k = 0; k < 2; k++) {
                  doc.addField(k == 0 ? Utils.FLD_OTHER_GIVENNAME : Utils.FLD_OTHER_SURNAME,
                               !Utils.isEmpty(namePieces[j][k]) ? namePieces[j][k] : Utils.UNKNOWN_NAME);
               }
            }
         }
         // store fullname(s)
         nodes = xml.query("image_data/person");
         if (nodes.size() > 0) {
            doc.addField(Utils.FLD_FULLNAME_STORED, PersonPageIndexer.getFullname((Element)nodes.get(0)));
         }
      }
   }
}
