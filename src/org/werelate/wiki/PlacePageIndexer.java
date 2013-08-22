package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.util.List;
import java.util.regex.Pattern;
import java.sql.SQLException;

import nu.xom.Document;
import nu.xom.Nodes;
import nu.xom.Node;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class PlacePageIndexer extends BasePageIndexer
{
   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_PLACE_NAME, "place/alternate_name/@name"),
      new IndexInstruction(Utils.FLD_PLACE_NAME, "place/type"),
      new IndexInstruction(Utils.FLD_PLACE_TYPE, "place/type"),
      new IndexInstruction(Utils.FLD_LATITUDE, "place/latitude"),
      new IndexInstruction(Utils.FLD_LONGITUDE, "place/longitude"),
   };

   public PlacePageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "place";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   private float getPlaceNameBoost(String title) {
      int commas = Utils.countOccurrences(',', title);
      if (commas >= 3) {
         return 0.8f;
      }
      else if (commas == 2) {
         return 0.9f;
      }
      return 1.0f;
   }

   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      // add fields from title
      String[] fields = title.split(",",2);
      doc.addField(Utils.FLD_PLACE_TITLE, title);
      float boost = getPlaceNameBoost(title);
      doc.addField(Utils.FLD_PLACE_NAME, fields[0], boost);
      String noUmlats = Utils.convertUmlats(fields[0]);
      if (noUmlats != null) {
         doc.addField(Utils.FLD_PLACE_NAME, noUmlats, boost);
      }

      if (fields.length > 1) {
         doc.addField(Utils.FLD_LOCATED_IN_PLACE, fields[1]);
      }
      doc.addField(Utils.FLD_OTHER_PLACE, title);
      if (xml != null) {
         Nodes nodes = xml.query("place/alternate_name/@name");
         for (int i = 0; i < nodes.size(); i++) {
            noUmlats = Utils.convertUmlats(nodes.get(i).getValue());
            if (noUmlats != null) {
               doc.addField(Utils.FLD_PLACE_NAME, noUmlats);
            }
         }

         nodes = xml.query("place/also_located_in/@place");
         for (int i = 0; i < nodes.size(); i++) {
            String placeText = nodes.get(i).getValue();
            int pos = placeText.indexOf('|');
            if (pos >= 0) {
               placeText = placeText.substring(0, pos);
            }
            // I don't think we need to index the ali
//               doc.addField(Utils.FLD_OTHER_PLACE, placeText);
            doc.addField(Utils.FLD_LOCATED_IN_PLACE, placeText);
            doc.addField(Utils.FLD_LOCATED_IN_PLACE_STORED, placeText);
         }
      }
      // I don't think it's that important to index the redirect target for this place page
//      if (redirTitle != null) {
//         doc.addField(Utils.FLD_OTHER_PLACE, redirTitle.substring("place:".length()));
//      }
   }
}
