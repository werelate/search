package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.sql.SQLException;

import nu.xom.Document;
import nu.xom.Nodes;

/**
 * Created by Dallan Quass
 * Date: Apr 25, 2008
 */
public class FamilyPageIndexer extends BasePageIndexer
{
   private static final String XPATH_HUSBAND_SURNAME = "family/husband/@surname";
   private static final String XPATH_HUSBAND_GIVENNAME = "family/husband/@given";
   private static final String XPATH_WIFE_SURNAME = "family/wife/@surname";
   private static final String XPATH_WIFE_GIVENNAME = "family/wife/@given";

   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_HUSBAND_SURNAME, XPATH_HUSBAND_SURNAME),
      new IndexInstruction(Utils.FLD_HUSBAND_GIVENNAME, XPATH_HUSBAND_GIVENNAME),
      new IndexInstruction(Utils.FLD_WIFE_SURNAME, XPATH_WIFE_SURNAME),
      new IndexInstruction(Utils.FLD_WIFE_GIVENNAME, XPATH_WIFE_GIVENNAME),
      new IndexInstruction(Utils.FLD_HUSBAND_BIRTH_DATE, "family/husband/@birthdate | family/husband/@chrdate", Utils.UNKNOWN_DATE),
      new IndexInstruction(Utils.FLD_HUSBAND_BIRTH_DATE_STORED, "family/husband[1]/@birthdate"),
      new IndexInstruction(Utils.FLD_HUSBAND_CHR_DATE_STORED, "family/husband[1]/@chrdate"),
      new IndexInstruction(Utils.FLD_HUSBAND_DEATH_DATE, "family/husband/@deathdate | family/husband/@burialdate", Utils.UNKNOWN_DATE),
      new IndexInstruction(Utils.FLD_HUSBAND_DEATH_DATE_STORED, "family/husband[1]/@deathdate"),
      new IndexInstruction(Utils.FLD_HUSBAND_BURIAL_DATE_STORED, "family/husband[1]/@burialdate"),
      new IndexInstruction(Utils.FLD_WIFE_BIRTH_DATE, "family/wife/@birthdate | family/wife/@chrdate", Utils.UNKNOWN_DATE),
      new IndexInstruction(Utils.FLD_WIFE_BIRTH_DATE_STORED, "family/wife[1]/@birthdate"),
      new IndexInstruction(Utils.FLD_WIFE_CHR_DATE_STORED, "family/wife[1]/@chrdate"),
      new IndexInstruction(Utils.FLD_WIFE_DEATH_DATE, "family/wife/@deathdate | family/wife/@burialdate", Utils.UNKNOWN_DATE),
      new IndexInstruction(Utils.FLD_WIFE_DEATH_DATE_STORED, "family/wife[1]/@deathdate"),
      new IndexInstruction(Utils.FLD_WIFE_BURIAL_DATE_STORED, "family/wife[1]/@burialdate"),
//      new IndexInstruction(Utils.FLD_HUSBAND_BIRTH_PLACE, "family/husband/@birthplace | family/husband/@chrplace", Utils.UNKNOWN_PLACE, IndexInstruction.FILTER_REMOVE_POST_BAR),
//      new IndexInstruction(Utils.FLD_HUSBAND_BIRTH_PLACE_STORED, "family/husband[1]/@birthplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
//      new IndexInstruction(Utils.FLD_HUSBAND_CHR_PLACE_STORED, "family/husband[1]/@chrplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
//      new IndexInstruction(Utils.FLD_HUSBAND_DEATH_PLACE, "family/husband/@deathplace | family/husband/@burialplace", Utils.UNKNOWN_PLACE, IndexInstruction.FILTER_REMOVE_POST_BAR),
//      new IndexInstruction(Utils.FLD_HUSBAND_DEATH_PLACE_STORED, "family/husband[1]/@deathplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
//      new IndexInstruction(Utils.FLD_HUSBAND_BURIAL_PLACE_STORED, "family/husband[1]/@burialplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
//      new IndexInstruction(Utils.FLD_WIFE_BIRTH_PLACE, "family/wife/@birthplace | family/wife/@chrplace", Utils.UNKNOWN_PLACE, IndexInstruction.FILTER_REMOVE_POST_BAR),
//      new IndexInstruction(Utils.FLD_WIFE_BIRTH_PLACE_STORED, "family/wife[1]/@birthplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
//      new IndexInstruction(Utils.FLD_WIFE_CHR_PLACE_STORED, "family/wife[1]/@chrplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
//      new IndexInstruction(Utils.FLD_WIFE_DEATH_PLACE, "family/wife/@deathplace | family/wife/@burialplace", Utils.UNKNOWN_PLACE, IndexInstruction.FILTER_REMOVE_POST_BAR),
//      new IndexInstruction(Utils.FLD_WIFE_DEATH_PLACE_STORED, "family/wife[1]/@deathplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
//      new IndexInstruction(Utils.FLD_WIFE_BURIAL_PLACE_STORED, "family/wife[1]/@burialplace", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_MARRIAGE_DATE, "family/event_fact[@type='Marriage' or @type='Marriage Banns' or @type='Alt Marriage']/@date", Utils.UNKNOWN_DATE),
      new IndexInstruction(Utils.FLD_MARRIAGE_DATE_STORED, "family/event_fact[@type='Marriage']/@date"),
      new IndexInstruction(Utils.FLD_BANNS_DATE_STORED, "family/event_fact[@type='Marriage Banns']/@date"),
      new IndexInstruction(Utils.FLD_MARRIAGE_PLACE, "family/event_fact[@type='Marriage' or @type='Marriage Banns'or @type='Alt Marriage']/@place", Utils.UNKNOWN_PLACE, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_MARRIAGE_PLACE_STORED, "family/event_fact[@type='Marriage']/@place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_BANNS_PLACE_STORED, "family/event_fact[@type='Marriage Banns']/@place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "family/event_fact[@type!='Marriage' and @type!='Marriage Banns'and @type!='Alt Marriage']/@place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
//      new IndexInstruction(Utils.FLD_HUSBAND_TITLE, "family/husband/@title"),
//      new IndexInstruction(Utils.FLD_WIFE_TITLE, "family/wife/@title"),
      new IndexInstruction(Utils.FLD_CHILD_TITLE, "family/child/@title"),
      new IndexInstruction(Utils.FLD_PRIMARY_IMAGE, "family/image[@primary='true']/@filename"),
   };

   public FamilyPageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "family";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   protected String getTitleSort(String title, Document xml) {
      return removeIndexNumber(title);
   }

   // default husband and wife names from title in case they don't exist
   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      if (xml != null) {
         String[][] namePieces = getFamilyNamePieces(removeIndexNumber(title));
         Nodes nodes = xml.query(XPATH_HUSBAND_GIVENNAME);
         if (nodes.size() == 0) {
            doc.addField(Utils.FLD_HUSBAND_GIVENNAME, !Utils.isEmpty(namePieces[0][0]) ? namePieces[0][0] : Utils.UNKNOWN_NAME);
         }
         nodes = xml.query(XPATH_HUSBAND_SURNAME);
         if (nodes.size() == 0) {
            doc.addField(Utils.FLD_HUSBAND_SURNAME, !Utils.isEmpty(namePieces[0][1]) ? namePieces[0][1] : Utils.UNKNOWN_NAME);
         }
         nodes = xml.query(XPATH_WIFE_GIVENNAME);
         if (nodes.size() == 0) {
            doc.addField(Utils.FLD_WIFE_GIVENNAME, !Utils.isEmpty(namePieces[1][0]) ? namePieces[1][0] : Utils.UNKNOWN_NAME);
         }
         nodes = xml.query(XPATH_WIFE_SURNAME);
         if (nodes.size() == 0) {
            doc.addField(Utils.FLD_WIFE_SURNAME, !Utils.isEmpty(namePieces[1][1]) ? namePieces[1][1] : Utils.UNKNOWN_NAME);
         }

         // store unsourced
         if (isUnsourced(xml, wikiContents)) {
            doc.addField(Utils.FLD_UNSOURCED, true);
         }
      }
   }
}
