package org.werelate.wiki;

import org.apache.solr.common.SolrInputDocument;
import org.folg.names.search.Normalizer;
import org.werelate.util.DatabaseConnectionHelper;
import org.werelate.util.Utils;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
   private static final Pattern YEAR_PATTERN = Pattern.compile("\\b(1\\d{3})\\b");

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

   private Normalizer normalizer;
   private Set<String> allSurnames = new HashSet<String>();
   private Set<String> allGivennames = new HashSet<String>();

   public PersonPageIndexer(DatabaseConnectionHelper conn) throws SQLException, IOException {
      super(conn);

      normalizer = Normalizer.getInstance();

      InputStream in = this.getClass().getClassLoader().getResourceAsStream("surnames200k.txt");
      BufferedReader r = new BufferedReader(new InputStreamReader(in, "UTF-8"));
      while (r.ready()) {
         allSurnames.add(r.readLine().toLowerCase());
      }
      in.close();

      in = this.getClass().getClassLoader().getResourceAsStream("givennames70k.txt");
      r = new BufferedReader(new InputStreamReader(in, "UTF-8"));
      while (r.ready()) {
         allGivennames.add(r.readLine().toLowerCase());
      }
      in.close();
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
         Set<String> marriedNames = new HashSet<String>();
         if (!"?".equals(gender)) {
            int j = ("M".equals(gender)) ? 1 : 0;
            nodes = xml.query("person/spouse_of_family/@title");
            for (int i = 0; i < nodes.size(); i++) {
               String[][] namePieces = getFamilyNamePieces(removeIndexNumber(nodes.get(i).getValue()));
               doc.addField(Utils.FLD_SPOUSE_GIVENNAME, !Utils.isEmpty(namePieces[j][0]) ? namePieces[j][0] : Utils.UNKNOWN_NAME);
               String spouseSurname = !Utils.isEmpty(namePieces[j][1]) ? namePieces[j][1] : Utils.UNKNOWN_NAME;
               doc.addField(Utils.FLD_SPOUSE_SURNAME, spouseSurname);
               // add married name for women
               if (j == 0 && !spouseSurname.equals(Utils.UNKNOWN_NAME)) {
                  marriedNames.add(spouseSurname);
               }
            }
         }
         for (String marriedName : marriedNames) {
            doc.addField(Utils.FLD_PERSON_MARRIED_NAME, marriedName);
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

         StringBuilder buf = new StringBuilder();

         // get name facets
         nodes = xml.query("person/name/@surname");
         for (int i = 0; i < nodes.size(); i++) {
            String name = nodes.get(i).getValue();
            if (name != null && name.length() > 0) {
               List<String> tokens = normalizer.normalize(name, true);
               if (tokens != null && tokens.size() > 0) {
                  for (String token : tokens) {
                     if (buf.length() > 0) {
                        buf.append("|");
                     }
                     if (allSurnames.contains(token)) {
                        token = Utils.toMixedCase(token);
                     }
                     else {
                        token = "Other";
                     }
                     buf.append(token);
                  }
               }
            }
         }
         if (buf.length() == 0) {
            buf.append("Unknown");
         }
         doc.addField(Utils.FLD_PERSON_SURNAME_FACET, buf.toString());

         nodes = xml.query("person/name/@given");
         buf.setLength(0);
         for (int i = 0; i < nodes.size(); i++) {
            String name = nodes.get(i).getValue();
            if (name != null && name.length() > 0) {
               List<String> tokens = normalizer.normalize(name, true);
               if (tokens != null && tokens.size() > 0) {
                  for (String token : tokens) {
                     if (buf.length() > 0) {
                        buf.append("|");
                     }
                     if (allGivennames.contains(token)) {
                        token = Utils.toMixedCase(token);
                     }
                     else {
                        token = "Other";
                     }
                     buf.append(token);
                  }
               }
            }
         }
         if (buf.length() == 0) {
            buf.append("Unknown");
         }
         doc.addField(Utils.FLD_PERSON_GIVENNAME_FACET, buf.toString());

         // get place facets
         nodes = xml.query("person/event_fact/@place");
         Set<String> countries = new HashSet<String>();
         Set<String> states = new HashSet<String>();
         for (int i = 0; i < nodes.size(); i++) {
            String place = nodes.get(i).getValue();
            if (place != null && place.length() > 0) {
               int pos = place.indexOf("|");
               if (pos >= 0) {
                  place = place.substring(0, pos);
               }
               if (place.length() > 0) {
                  String[] levels = place.split("\\s*,\\s*");
                  if (levels.length > 0) {
                     countries.add(levels[levels.length-1]);
                  }
                  if (levels.length > 1) {
                     states.add(levels[levels.length-1]+", "+levels[levels.length-2]);
                  }
               }
            }
         }
         if (countries.size() > 0) {
            buf.setLength(0);
            for (String p : countries) {
               if (buf.length() > 0) {
                  buf.append("|");
               }
               buf.append(p);
            }
            doc.addField(Utils.FLD_PERSON_COUNTRY_FACET, buf.toString());
         }
         if (states.size() > 0) {
            buf.setLength(0);
            for (String p : states) {
               if (buf.length() > 0) {
                  buf.append("|");
               }
               buf.append(p);
            }
            doc.addField(Utils.FLD_PERSON_STATE_FACET, buf.toString());
         }

         // get date facets
         nodes = xml.query("person/event_fact[@type='Birth']/@date");
         for (int i = 0; i < nodes.size(); i++) {
            String date = nodes.get(i).getValue();
            if (date != null && date.length() > 0) {
               // find a 4-digit year and turn it into a century (pre1600,...,1900) and a decade (1600..1990)
               Matcher m = YEAR_PATTERN.matcher(date);
               if (m.find()) {
                  String year = m.group(1);
                  String century = year.substring(0,2);
                  String decade = year.substring(0,3);
                  if (century.compareTo("16") < 0) {
                     century = "pre1600";
                     decade = "";
                  }
                  else {
                     century += "00";
                     decade += "0";
                  }
                  doc.addField(Utils.FLD_PERSON_CENTURY_FACET, century);
                  if (decade.length() > 0) {
                     doc.addField(Utils.FLD_PERSON_DECADE_FACET, decade);
                  }
               }
            }
         }
      }
   }
}
