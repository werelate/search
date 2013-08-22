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
public class PersonPageIndexer extends BasePageIndexer
{
   private static final String XPATH_PERSON_SURNAME = "person/name/@surname | person/alt_name/@surname | person/name/@title_suffix | person/alt_name/@title_suffix";
   private static final String XPATH_PERSON_GIVENNAME = "person/name/@given | person/alt_name/@given | person/name/@title_prefix | person/alt_name/@title_prefix";

   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_PERSON_SURNAME, XPATH_PERSON_SURNAME),
      new IndexInstruction(Utils.FLD_PERSON_GIVENNAME, XPATH_PERSON_GIVENNAME),
      new IndexInstruction(Utils.FLD_PERSON_BIRTH_DATE, "person/event_fact[@type='Birth' or @type='Baptism' or @type='Christening' or @type='Alt Birth' or @type='Alt Christening']/@date", Utils.UNKNOWN_DATE),
      new IndexInstruction(Utils.FLD_PERSON_BIRTH_DATE_STORED, "person/event_fact[@type='Birth']/@date"),
      new IndexInstruction(Utils.FLD_PERSON_CHR_DATE_STORED, "person/event_fact[@type='Christening'or @type='Baptism' ]/@date"),
      new IndexInstruction(Utils.FLD_PERSON_BIRTH_PLACE, "person/event_fact[@type='Birth' or @type='Baptism' or @type='Christening' or @type='Alt Birth' or @type='Alt Christening']/@place", Utils.UNKNOWN_PLACE, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_PERSON_BIRTH_PLACE_STORED, "person/event_fact[@type='Birth']/@place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_PERSON_CHR_PLACE_STORED, "person/event_fact[@type='Christening'or @type='Baptism' ]/@place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_PERSON_DEATH_DATE, "person/event_fact[@type='Death' or @type='Burial' or @type='Alt Death' or @type='Alt Burial']/@date", Utils.UNKNOWN_DATE),
      new IndexInstruction(Utils.FLD_PERSON_DEATH_DATE_STORED, "person/event_fact[@type='Death']/@date"),
      new IndexInstruction(Utils.FLD_PERSON_BURIAL_DATE_STORED, "person/event_fact[@type='Burial']/@date"),
      new IndexInstruction(Utils.FLD_PERSON_DEATH_PLACE, "person/event_fact[@type='Death' or @type='Burial' or @type='Alt Death' or @type='Alt Burial']/@place", Utils.UNKNOWN_PLACE, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_PERSON_DEATH_PLACE_STORED, "person/event_fact[@type='Death']/@place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_PERSON_BURIAL_PLACE_STORED, "person/event_fact[@type='Burial']/@place", null, IndexInstruction.FILTER_REMOVE_PRE_BAR),
      new IndexInstruction(Utils.FLD_OTHER_PLACE, "person/event_fact[@type!='Birth' and @type!='Baptism' and @type!='Christening' and @type!='Alt Birth' and @type!='Alt Christening' and @type!='Death' and @type!='Burial' and @type!='Alt Death' and @type!='Alt Burial']/@place", null, IndexInstruction.FILTER_REMOVE_POST_BAR),
      new IndexInstruction(Utils.FLD_PARENT_FAMILY_TITLE, "person/child_of_family/@title"),
      new IndexInstruction(Utils.FLD_SPOUSE_FAMILY_TITLE, "person/spouse_of_family/@title"),
      new IndexInstruction(Utils.FLD_PRIMARY_IMAGE, "person/image[@primary='true']/@filename"),
   };

   public PersonPageIndexer(DatabaseConnectionHelper conn) throws SQLException
   {
      super(conn);
   }

   protected String getTagName() {
      return "person";
   }

   protected IndexInstruction[] getIndexInstructions() {
      return INSTRUCTIONS;
   }

   private static final String[][] PARENT_NAMES = {
      {Utils.FLD_FATHER_GIVENNAME, Utils.FLD_FATHER_SURNAME},
      {Utils.FLD_MOTHER_GIVENNAME, Utils.FLD_MOTHER_SURNAME},
   };

   protected String getTitleSort(String title, Document xml) {
      if (xml != null) {
         Nodes nodes  = xml.query("person/name");
         if (nodes.size() > 0) {
            String name = getFullname((Element)nodes.get(0));
            if (name.indexOf(' ') > 0) {
               // TODO remove
               return "Person:"+name;
            }
         }
      }
      return removeIndexNumber(title);
   }

   // add parents' names, spouse name, default person name, store fullname
   protected void addCustomFields(SolrInputDocument doc, String title, Document xml, String wikiContents, String redirTitle) {
      if (xml != null) {
         // get parents names
         Nodes nodes = xml.query("person/child_of_family/@title");
         for (int i = 0; i < nodes.size(); i++) {
            String[][] namePieces = getFamilyNamePieces(removeIndexNumber(nodes.get(i).getValue()));
            for (int j = 0; j < 2; j++) {
               for (int k = 0; k < 2; k++) {
                  doc.addField(PARENT_NAMES[j][k], !Utils.isEmpty(namePieces[j][k]) ? namePieces[j][k] : Utils.UNKNOWN_NAME);
               }
            }
         }
         // default parents if they don't exist
         if (nodes.size() == 0) {
            for (int i = 0; i < 2; i++) {
               for (int j = 0; j < 2; j++) {
                  doc.addField(PARENT_NAMES[i][j], Utils.UNKNOWN_NAME);
               }
            }
         }

         // get gender
         nodes = xml.query("person/gender");
         String gender = "?";
         if (nodes.size() > 0) {
            gender = nodes.get(0).getValue();
         }
         // get spouse name
         if (!"?".equals(gender)) {
            int j = ("M".equals(gender)) ? 1 : 0;
            nodes = xml.query("person/spouse_of_family/@title");
            for (int i = 0; i < nodes.size(); i++) {
               String[][] namePieces = getFamilyNamePieces(removeIndexNumber(nodes.get(i).getValue()));
               doc.addField(Utils.FLD_SPOUSE_GIVENNAME, !Utils.isEmpty(namePieces[j][0]) ? namePieces[j][0] : Utils.UNKNOWN_NAME);
               doc.addField(Utils.FLD_SPOUSE_SURNAME, !Utils.isEmpty(namePieces[j][1]) ? namePieces[j][1] : Utils.UNKNOWN_NAME);
            }
         }
         // default spouse if it doesn't exist
         if (nodes.size() == 0 || "?".equals(gender)) {
            doc.addField(Utils.FLD_SPOUSE_SURNAME, Utils.UNKNOWN_NAME);
            doc.addField(Utils.FLD_SPOUSE_GIVENNAME, Utils.UNKNOWN_NAME);
         }

         // default person name if needed
         String[] names = removeIndexNumber(title).split(" ", 2);
         nodes = xml.query(XPATH_PERSON_SURNAME);
         if (nodes.size() == 0) {
            doc.addField(Utils.FLD_PERSON_SURNAME, names.length == 2 ? names[1] : Utils.UNKNOWN_NAME);
         }
         nodes = xml.query(XPATH_PERSON_GIVENNAME);
         if (nodes.size() == 0) {
            doc.addField(Utils.FLD_PERSON_GIVENNAME, names.length >= 1 ? names[0] : Utils.UNKNOWN_NAME);
         }

         // store fullname
         nodes = xml.query("person/name");
         if (nodes.size() > 0) {
            doc.addField(Utils.FLD_FULLNAME_STORED, getFullname((Element)nodes.get(0)));
         }

         // store unsourced
         if (isUnsourced(xml, wikiContents)) {
            doc.addField(Utils.FLD_UNSOURCED, true);
         }
      }
   }
}
