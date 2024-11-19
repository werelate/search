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
   private static final Pattern YEAR_PATTERN = Pattern.compile("\\b([012]?\\d{3})\\b");  // changed to include 3-digit years and 2xxx (Dec 2020 by Janet Bjorndahl)

   private static final IndexInstruction[] INSTRUCTIONS = {
      new IndexInstruction(Utils.FLD_PERSON_SURNAME, XPATH_PERSON_SURNAME),
      new IndexInstruction(Utils.FLD_PERSON_GIVENNAME, XPATH_PERSON_GIVENNAME),
      new IndexInstruction(Utils.FLD_PERSON_GENDER, "person/gender"),
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
   private String pageSortName = null;

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

   /* Override base method. Note that this also stores the sort name for use in creating the surname index facet. */
   protected String getTitleSort(String fullTitle, Document xml) {
      if (xml != null) {
         Nodes nodes  = xml.query("person/name");
         if (nodes.size() > 0) {
            Element name = (Element)nodes.get(0);
            if (!Utils.isEmpty(getNameAttr(name, "surname")) || !Utils.isEmpty(getNameAttr(name, "given"))) {
               pageSortName = getReversedSortName(name);
               return "Person:" + pageSortName;
            }
         }
      }
      pageSortName = getReversedTitle(fullTitle.substring(7));
      return "Person:" + pageSortName;
   }

   /* Name for sorting, starting with surname. Excludes prefix and suffix.
      If both given and surname are unknown, return just "Unknown". */
   protected static String getReversedSortName(Element name) {
      StringBuilder buf = new StringBuilder();

      String surname = getNameAttr(name, "surname");
      String given = getNameAttr(name, "given");
      if (!Utils.isEmpty(surname) && !surname.toLowerCase().equals("unknown")) {
         appendAttr(surname, buf);
      }
      else {
         appendAttr("Unknown", buf);
         if (Utils.isEmpty(given) || given.toLowerCase().equals("unknown")) {
            return buf.toString();
         }
      }
      appendAttr(",", buf);
      if (!Utils.isEmpty(given)) {
         appendAttr(given, buf);
      }
      else {
         appendAttr("Unknown", buf);
      }
      return buf.toString();
   }

   /* Name for sorting or display, starting with surname - from the page title for when there are no name fields.
      If both given and surname are unknown, return just "Unknown". */
   protected static String getReversedTitle(String titleName) {
      StringBuilder buf = new StringBuilder();
      String[] namePieces = removeIndexNumber(titleName).split(" ", 2);
      if (namePieces.length>1 && !namePieces[1].toLowerCase().equals("unknown")) {
         appendAttr(namePieces[1], buf);
      }
      else {
         appendAttr("Unknown", buf);
         if (namePieces[0].toLowerCase().equals("unknown")) {
            return buf.toString();
         }
      }
      appendAttr(",", buf);
      appendAttr(namePieces[0], buf);
      return buf.toString(); 
   }

   /* Name for display, starting with surname. Includes prefix and suffix.
      If both given and surname are unknown, return just "Unknown". */
   protected static String getReversedFullname(Element name) {
      StringBuilder buf = new StringBuilder();
      if (!Utils.isEmpty(getNameAttr(name, "surname"))) {
         appendAttr(getNameAttr(name, "surname"), buf);
      }
      else {
         appendAttr("Unknown", buf);
      }
      appendAttr(",", buf);
      appendAttr(getNameAttr(name, "title_prefix"), buf);
      if (!Utils.isEmpty(getNameAttr(name, "given"))) {
         appendAttr(getNameAttr(name, "given"), buf);
      }
      else {
         appendAttr("Unknown", buf);
      }
      if (!Utils.isEmpty(getNameAttr(name, "title_suffix"))) {
         appendAttr(",",buf);
         appendAttr(getNameAttr(name, "title_suffix"), buf);
      }
      if (buf.toString().toLowerCase().equals("unknown, unknown")) {
         return "Unknown";
      }
      return buf.toString();
}

   protected static String getFullname(Element name) {
      StringBuilder buf = new StringBuilder();
      appendAttr(getNameAttr(name, "title_prefix"), buf);
      appendAttr(!Utils.isEmpty(getNameAttr(name, "given")) ? getNameAttr(name, "given") : "___", buf);
      appendAttr(getNameAttr(name, "surname"), buf);
      appendAttr(getNameAttr(name, "title_suffix"), buf);
      return buf.toString();
   }

   /* First letter of surname for the surname index.
      This uses the saved page sort name, which begins with the surname as found in the XML or the page title.
      If no letters in surname, return "U" for Unknown. If surname cannot be romanized, return "other". */
   protected static String getSurnameIndex(String pageSortName) {
      String index = null;
      String[] split = pageSortName.split(",",2);
      String name = Utils.romanize(split[0]);
      if (name != null && name.length() > 0) {
         for (int i=0; name.length()>i; i++) {
            char c = Character.toUpperCase(name.charAt(i));
            if (c >= 'A' && c <= 'Z') {
               index = String.valueOf(c);
               break;
            }
            else if ((int)c > 127) {  // a character that could not be romanized
               index = "other";
               break;
            }
         }
      }
      if (index == null) { // no alphabetic characters
         index = "U";
      }
      return index;
   }
         
   /* Get the trimmed value of a part of the name. */
   protected static String getNameAttr(Element name, String attr) {
      String nameAttr = name.getAttributeValue(attr);
      if (nameAttr!=null) {
         nameAttr = nameAttr.trim();
      }
      return nameAttr;
   }

   protected static void appendAttr(String attr, StringBuilder buf) {
      if (!Utils.isEmpty(attr)) {
         if (buf.length() > 0 && !attr.equals(",")) {
            buf.append(" ");
         }
         buf.append(attr);
      }
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

         // add initials of given names (not prefixes, as there is no value in searching on initials of a prefix)
         else {
            for (int i=0; i < nodes.size(); i++) {
               if (nodes.get(i).toString().contains("given=")) {
                  String allGiven = nodes.get(i).getValue();
                  String initials = "";
                  for (String s : allGiven.split(" ")) {
                     if (s.length() > 0) {
                        initials += initials.length()>0 ? " " + s.charAt(0) : s.charAt(0);
                     }
                  }
                  if (initials.length() > 0) {
                     doc.addField(Utils.FLD_PERSON_GIVENNAME, initials);
                  }
               }
            }
         }

         // store fullname (surname first)
         nodes = xml.query("person/name");
         if (nodes.size() > 0) {
            doc.addField(Utils.FLD_FULLNAME_STORED, getReversedFullname((Element)nodes.get(0)));
         }
         else {
            doc.addField(Utils.FLD_FULLNAME_STORED, getReversedTitle(title));
         }

         // store fullname of alt names, except for married surnames already captured
         nodes = xml.query("person/alt_name");
         for (int i = 0, len = 0; i < nodes.size() && len < /* Utils.MAX_ALTNAME_LENGTH */ 100; i++) {
            Element name = (Element)nodes.get(i);
            if (name.getAttributeValue("surname") == null || name.getAttributeValue("type") == null || 
                  !(name.getAttributeValue("type").equals("Married Name") && marriedNames.contains(name.getAttributeValue("surname")))) {
               String nameType = null;
               nameType = name.getAttributeValue("type");
               if (nameType == null) {
                  nameType = "U";
               }
               else {
                  nameType = nameType.substring(0,1);
               }
               doc.addField(Utils.FLD_ALTNAME_STORED, getFullname(name));
               len += getFullname(name).length();
            }
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

         /* get surname index facet (first alphabetic letter of the page sort name) 
            Note: This relies on getTitleSort being run first. */
         doc.addField(Utils.FLD_SURNAME_INDEX_FACET, getSurnameIndex(pageSortName));

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
                     String country = levels[levels.length-1];
                     if (Utils.COUNTRIES.contains(country)) {
                        countries.add(country);
                        if (levels.length > 1) {
                           states.add(country+", "+levels[levels.length-2]);
                        }
                        else {
                           states.add(country+", "+"Unknown");
                        }
                     }
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
         else {
            doc.addField(Utils.FLD_PERSON_COUNTRY_FACET, "Unknown");
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
         if (nodes.size() == 0) {
           nodes = xml.query("person/event_fact[@type='Christening']/@date");   // use Christening date if no Birth date (added Dec 2020 by Janet Bjorndahl)
         }  
         Set<String> centuries = new HashSet<String>();
         Set<String> decades = new HashSet<String>();
         for (int i = 0; i < nodes.size(); i++) {
            String date = nodes.get(i).getValue();
            if (date != null && date.length() > 0) {
               // find a 3- or 4-digit year and turn it into a century (pre1600,...,1900,2000) and a decade (1600..2020)
               Matcher m = YEAR_PATTERN.matcher(date);
               if (m.find()) {
                  String year = m.group(1);
                  String century = year.substring(0,year.length()-2);         // updated to handle 3-digit years (Dec 2020 by Janet Bjorndahl)
                  String decade = year.substring(0,year.length()-1);          // updated to handle 3-digit years (Dec 2020 by Janet Bjorndahl)
                  if (century.compareTo("16") < 0 || century.length()==1) {   // updated to handle 3-digit years (Dec 2020 by Janet Bjorndahl)
                     century = "pre1600";
                     decade = "";
                  }
                  else {
                     century += "00";
                     decade += "0";
                  }
                  centuries.add(century);
                  if (decade.length() > 0) {
                     decades.add(decade);
                  }
               }
            }
         }
         if (centuries.size() > 0) {
            buf.setLength(0);
            for (String c : centuries) {
               if (buf.length() > 0) {
                  buf.append("|");
               }
               buf.append(c);
            }
            doc.addField(Utils.FLD_PERSON_CENTURY_FACET, buf.toString());
         }
         else {
            doc.addField(Utils.FLD_PERSON_CENTURY_FACET, "Unknown");
         }
         if (decades.size() > 0) {
            buf.setLength(0);
            for (String d : decades) {
               if (buf.length() > 0) {
                  buf.append("|");
               }
               buf.append(d);
            }
            doc.addField(Utils.FLD_PERSON_DECADE_FACET, buf.toString());
         }
      }
   }
}
