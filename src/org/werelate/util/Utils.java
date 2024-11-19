package org.werelate.util;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import nu.xom.ParsingException;
import nu.xom.Builder;
import nu.xom.Document;
import com.ibm.icu.text.Normalizer;

import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.net.URLEncoder;

/**
 * Created by Dallan Quass
 * Date: Apr 23, 2008
 */
public class Utils
{
   public static final String UNKNOWN_NAME = "unknown";
   public static final String UNKNOWN_DATE = "0000";
   public static final String UNKNOWN_PLACE = "unknown";
   public static final String LIVING_NAME = "living";
   public static final String NO_SUBPLACE = "(no subordinate)";

   public static final String FLD_TITLE = "Title";
   public static final String FLD_TITLE_STORED = "TitleStored";
   public static final String FLD_TITLE_SORT_VALUE = "TitleSortValue";
   public static final String FLD_TITLE_FIRST_LETTER = "TitleFirstLetter";
   public static final String FLD_NAMESPACE = "Namespace";
   public static final String FLD_NAMESPACE_STORED = "NamespaceStored";
   public static final String FLD_TALK_NAMESPACE = "TalkNamespace";
   public static final String FLD_UNSOURCED = "Unsourced";
   public static final String FLD_PLACE0 = "Place0";
   public static final String FLD_PLACE1 = "Place1";
   public static final String FLD_PLACE2 = "Place2";
   public static final String FLD_LAST_MOD_DATE = "LastModDate";
   public static final String FLD_PAGE_ID = "PageId";
   public static final String FLD_USER = "User";
   public static final String FLD_USER_STORED = "UserStored";
   public static final String FLD_TREE = "Tree";
   public static final String FLD_CATEGORY = "Category";
   public static final String FLD_KEYWORDS = "Keywords";
   public static final String FLD_TEXT_STORED = "TextStored";
   public static final String FLD_POPULARITY = "Popularity";

   public static final String FLD_PERSON_SURNAME = "PersonSurname";
   public static final String FLD_PERSON_GIVENNAME = "PersonGivenname";
   public static final String FLD_PERSON_GENDER = "PersonGender";
   public static final String FLD_PERSON_BIRTH_DATE = "PersonBirthDate";
   public static final String FLD_PERSON_DEATH_DATE = "PersonDeathDate";
   public static final String FLD_PERSON_BIRTH_YEAR = "PersonBirthYear";
   public static final String FLD_PERSON_DEATH_YEAR = "PersonDeathYear";
   public static final String FLD_PERSON_BIRTH_PLACE = "PersonBirthPlace";
   public static final String FLD_PERSON_DEATH_PLACE = "PersonDeathPlace";
   public static final String FLD_FATHER_SURNAME = "FatherSurname";
   public static final String FLD_FATHER_GIVENNAME = "FatherGivenname";
   public static final String FLD_MOTHER_SURNAME = "MotherSurname";
   public static final String FLD_MOTHER_GIVENNAME = "MotherGivenname";
   public static final String FLD_SPOUSE_SURNAME = "SpouseSurname";
   public static final String FLD_SPOUSE_GIVENNAME = "SpouseGivenname";
   public static final String FLD_PARENT_FAMILY_TITLE = "ParentFamilyTitle";
   public static final String FLD_SPOUSE_FAMILY_TITLE = "SpouseFamilyTitle";
   public static final String FLD_PERSON_MARRIED_NAME = "PersonMarriedName";
   public static final String FLD_PERSON_GIVENNAME_FACET = "PersonGivennameFacet";
   public static final String FLD_PERSON_SURNAME_FACET = "PersonSurnameFacet";
   public static final String FLD_PERSON_COUNTRY_FACET = "PersonCountryFacet";
   public static final String FLD_PERSON_STATE_FACET = "PersonStateFacet";
   public static final String FLD_PERSON_CENTURY_FACET = "PersonCenturyFacet";
   public static final String FLD_PERSON_DECADE_FACET = "PersonDecadeFacet";
   public static final String FLD_PERSON_BIRTH_DATE_STORED = "PersonBirthDateStored";
   public static final String FLD_PERSON_CHR_DATE_STORED = "PersonChrDateStored";
   public static final String FLD_PERSON_DEATH_DATE_STORED = "PersonDeathDateStored";
   public static final String FLD_PERSON_BURIAL_DATE_STORED = "PersonBurialDateStored";
   public static final String FLD_PERSON_BIRTH_PLACE_STORED = "PersonBirthPlaceStored";
   public static final String FLD_PERSON_CHR_PLACE_STORED = "PersonChrPlaceStored";
   public static final String FLD_PERSON_DEATH_PLACE_STORED = "PersonDeathPlaceStored";
   public static final String FLD_PERSON_BURIAL_PLACE_STORED = "PersonBurialPlaceStored";

   public static final String FLD_SURNAME_INDEX_FACET = "SurnameIndexFacet";

   public static final String FLD_HUSBAND_SURNAME = "HusbandSurname";
   public static final String FLD_HUSBAND_GIVENNAME = "HusbandGivenname";
   public static final String FLD_WIFE_SURNAME = "WifeSurname";
   public static final String FLD_WIFE_GIVENNAME = "WifeGivenname";
   public static final String FLD_MARRIAGE_DATE = "MarriageDate";
   public static final String FLD_MARRIAGE_YEAR = "MarriageYear";
   public static final String FLD_MARRIAGE_PLACE = "MarriagePlace";
   public static final String FLD_HUSBAND_BIRTH_DATE = "HusbandBirthDate";
   public static final String FLD_HUSBAND_DEATH_DATE = "HusbandDeathDate";
   public static final String FLD_WIFE_BIRTH_DATE = "WifeBirthDate";
   public static final String FLD_WIFE_DEATH_DATE = "WifeDeathDate";
   public static final String FLD_HUSBAND_BIRTH_YEAR = "HusbandBirthYear";
   public static final String FLD_HUSBAND_DEATH_YEAR = "HusbandDeathYear";
   public static final String FLD_WIFE_BIRTH_YEAR = "WifeBirthYear";
   public static final String FLD_WIFE_DEATH_YEAR = "WifeDeathYear";
//   public static final String FLD_HUSBAND_BIRTH_PLACE = "HusbandBirthPlace";
//   public static final String FLD_HUSBAND_DEATH_PLACE = "HusbandDeathPlace";
//   public static final String FLD_WIFE_BIRTH_PLACE = "WifeBirthPlace";
//   public static final String FLD_WIFE_DEATH_PLACE = "WifeDeathPlace";
//   public static final String FLD_HUSBAND_TITLE = "HusbandTitle";
//   public static final String FLD_WIFE_TITLE = "WifeTitle";
   public static final String FLD_CHILD_TITLE = "ChildTitle";
   public static final String FLD_MARRIAGE_DATE_STORED = "MarriageDateStored";
   public static final String FLD_BANNS_DATE_STORED = "BannsDateStored";
   public static final String FLD_MARRIAGE_PLACE_STORED = "MarriagePlaceStored";
   public static final String FLD_BANNS_PLACE_STORED = "BannsPlaceStored";
   public static final String FLD_HUSBAND_BIRTH_DATE_STORED = "HusbandBirthDateStored";
   public static final String FLD_HUSBAND_CHR_DATE_STORED = "HusbandChrDateStored";
   public static final String FLD_HUSBAND_DEATH_DATE_STORED = "HusbandDeathDateStored";
   public static final String FLD_HUSBAND_BURIAL_DATE_STORED = "HusbandBurialDateStored";
   public static final String FLD_WIFE_BIRTH_DATE_STORED = "WifeBirthDateStored";
   public static final String FLD_WIFE_CHR_DATE_STORED = "WifeChrDateStored";
   public static final String FLD_WIFE_DEATH_DATE_STORED = "WifeDeathDateStored";
   public static final String FLD_WIFE_BURIAL_DATE_STORED = "WifeBurialDateStored";
//   public static final String FLD_HUSBAND_BIRTH_PLACE_STORED = "HusbandBirthPlaceStored";
//   public static final String FLD_HUSBAND_CHR_PLACE_STORED = "HusbandChrPlaceStored";
//   public static final String FLD_HUSBAND_DEATH_PLACE_STORED = "HusbandDeathPlaceStored";
//   public static final String FLD_HUSBAND_BURIAL_PLACE_STORED = "HusbandBurialPlaceStored";
//   public static final String FLD_WIFE_BIRTH_PLACE_STORED = "WifeBirthPlaceStored";
//   public static final String FLD_WIFE_CHR_PLACE_STORED = "WifeChrPlaceStored";
//   public static final String FLD_WIFE_DEATH_PLACE_STORED = "WifeDeathPlaceStored";
//   public static final String FLD_WIFE_BURIAL_PLACE_STORED = "WifeBurialPlaceStored";

   public static final String FLD_OTHER_SURNAME = "OtherSurname";
   public static final String FLD_OTHER_GIVENNAME = "OtherGivenname";
   public static final String FLD_OTHER_PLACE = "OtherPlace";
   public static final String FLD_SURNAME_STORED = "SurnameStored";
   public static final String FLD_FULLNAME_STORED = "FullnameStored";   // fullname/familyTitle in images; fullname on Person page
   public static final String FLD_ALTNAME_STORED = "AltnameStored";
   public static final String FLD_PLACE_STORED = "PlaceStored";
   public static final String FLD_PRIMARY_IMAGE = "PrimaryImage";
   public static final String FLD_FROM_YEAR = "FromYear";
   public static final String FLD_TO_YEAR = "ToYear";

   public static final String FLD_AUTHOR = "Author";
   public static final String FLD_SOURCE_SUBJECT = "SourceSubject";
   public static final String FLD_SOURCE_SUBJECT_STORED = "SourceSubjectStored";
   public static final String FLD_SOURCE_SUB_SUBJECT = "SourceSubSubject";
   public static final String FLD_SOURCE_AVAILABILITY = "SourceAvailability";
   public static final String FLD_SOURCE_AVAILABILITY_STORED = "SourceAvailabilityStored";
   public static final String FLD_AUTHOR_STORED = "AuthorStored";
   public static final String FLD_SOURCE_TITLE_STORED = "SourceTitleStored";

   public static final String FLD_SURNAME_TITLE = "SurnameTitle";
   public static final String FLD_GIVENNAME_TITLE = "GivennameTitle";
   public static final String FLD_RELATED_NAME= "RelatedName";

   public static final String FLD_PLACE_TITLE = "PlaceTitle";
   public static final String FLD_PLACE_NAME = "PlaceName";
   public static final String FLD_LOCATED_IN_PLACE = "LocatedInPlace";
   public static final String FLD_LOCATED_IN_PLACE_STORED = "LocatedInPlaceStored";
   public static final String FLD_PLACE_TYPE = "PlaceType";
   public static final String FLD_LATITUDE = "Latitude";
   public static final String FLD_LONGITUDE = "Longitude";

   public static final String FLD_SURNAME = "Surname";
   public static final String FLD_GIVENNAME = "Givenname";
   public static final String FLD_PLACE = "Place";

   public static final Map<String,String[]> HIGHLIGHT_FIELDS = new HashMap<String,String[]>();
   static {
      HIGHLIGHT_FIELDS.put(FLD_TITLE, new String[]{FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_USER, new String[]{FLD_USER_STORED});
      HIGHLIGHT_FIELDS.put(FLD_KEYWORDS, new String[]{FLD_TEXT_STORED,FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_SURNAME, new String[]{FLD_FULLNAME_STORED,FLD_TITLE_STORED,FLD_ALTNAME_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_GIVENNAME, new String[]{FLD_FULLNAME_STORED,FLD_TITLE_STORED,FLD_ALTNAME_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_GENDER, new String[]{FLD_PERSON_GENDER});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_BIRTH_DATE, new String[]{FLD_PERSON_BIRTH_DATE_STORED, FLD_PERSON_CHR_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_BIRTH_YEAR, new String[]{FLD_PERSON_BIRTH_DATE_STORED, FLD_PERSON_CHR_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_BIRTH_PLACE, new String[]{FLD_PERSON_BIRTH_PLACE_STORED, FLD_PERSON_CHR_PLACE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_DEATH_DATE, new String[]{FLD_PERSON_DEATH_DATE_STORED, FLD_PERSON_BURIAL_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_DEATH_YEAR, new String[]{FLD_PERSON_DEATH_DATE_STORED, FLD_PERSON_BURIAL_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PERSON_DEATH_PLACE, new String[]{FLD_PERSON_DEATH_PLACE_STORED, FLD_PERSON_BURIAL_PLACE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_FATHER_SURNAME, new String[]{FLD_PARENT_FAMILY_TITLE});
      HIGHLIGHT_FIELDS.put(FLD_FATHER_GIVENNAME, new String[]{FLD_PARENT_FAMILY_TITLE});
      HIGHLIGHT_FIELDS.put(FLD_MOTHER_SURNAME, new String[]{FLD_PARENT_FAMILY_TITLE});
      HIGHLIGHT_FIELDS.put(FLD_MOTHER_GIVENNAME, new String[]{FLD_PARENT_FAMILY_TITLE});
      HIGHLIGHT_FIELDS.put(FLD_SPOUSE_SURNAME, new String[]{FLD_SPOUSE_FAMILY_TITLE});
      HIGHLIGHT_FIELDS.put(FLD_SPOUSE_GIVENNAME, new String[]{FLD_SPOUSE_FAMILY_TITLE});
      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_SURNAME, new String[]{FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_GIVENNAME, new String[]{FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_WIFE_SURNAME, new String[]{FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_WIFE_GIVENNAME, new String[]{FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_MARRIAGE_DATE, new String[]{FLD_MARRIAGE_DATE_STORED, FLD_BANNS_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_MARRIAGE_YEAR, new String[]{FLD_MARRIAGE_DATE_STORED, FLD_BANNS_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_MARRIAGE_PLACE, new String[]{FLD_MARRIAGE_PLACE_STORED, FLD_BANNS_PLACE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_BIRTH_DATE, new String[]{FLD_HUSBAND_BIRTH_DATE_STORED, FLD_HUSBAND_CHR_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_BIRTH_YEAR, new String[]{FLD_HUSBAND_BIRTH_DATE_STORED, FLD_HUSBAND_CHR_DATE_STORED});
//      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_BIRTH_PLACE, new String[]{FLD_HUSBAND_BIRTH_PLACE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_DEATH_DATE, new String[]{FLD_HUSBAND_DEATH_DATE_STORED, FLD_HUSBAND_BURIAL_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_DEATH_YEAR, new String[]{FLD_HUSBAND_DEATH_DATE_STORED, FLD_HUSBAND_BURIAL_DATE_STORED});
//      HIGHLIGHT_FIELDS.put(FLD_HUSBAND_DEATH_PLACE, new String[]{FLD_HUSBAND_DEATH_PLACE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_WIFE_BIRTH_DATE, new String[]{FLD_WIFE_BIRTH_DATE_STORED, FLD_WIFE_CHR_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_WIFE_BIRTH_YEAR, new String[]{FLD_WIFE_BIRTH_DATE_STORED, FLD_WIFE_CHR_DATE_STORED});
//      HIGHLIGHT_FIELDS.put(FLD_WIFE_BIRTH_PLACE, new String[]{FLD_WIFE_BIRTH_PLACE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_WIFE_DEATH_DATE, new String[]{FLD_WIFE_DEATH_DATE_STORED, FLD_WIFE_BURIAL_DATE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_WIFE_DEATH_YEAR, new String[]{FLD_WIFE_DEATH_DATE_STORED, FLD_WIFE_BURIAL_DATE_STORED});
//      HIGHLIGHT_FIELDS.put(FLD_WIFE_DEATH_PLACE, new String[]{FLD_WIFE_DEATH_PLACE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_OTHER_SURNAME, new String[]{FLD_SURNAME_STORED,FLD_FULLNAME_STORED,FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_OTHER_GIVENNAME, new String[]{FLD_FULLNAME_STORED,FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_OTHER_PLACE, new String[]{FLD_PLACE_STORED,FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PLACE_NAME, new String[]{FLD_TITLE_STORED,FLD_PLACE_TYPE});
      HIGHLIGHT_FIELDS.put(FLD_LOCATED_IN_PLACE, new String[]{FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_SURNAME, new String[]{FLD_SURNAME_STORED,FLD_FULLNAME_STORED,FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_GIVENNAME, new String[]{FLD_FULLNAME_STORED,FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_PLACE, new String[]{FLD_PERSON_BIRTH_PLACE_STORED,FLD_PERSON_DEATH_PLACE_STORED,FLD_MARRIAGE_PLACE_STORED,FLD_PLACE_STORED,FLD_TITLE_STORED});
      HIGHLIGHT_FIELDS.put(FLD_SOURCE_SUBJECT, new String[]{FLD_SOURCE_SUBJECT_STORED});
      HIGHLIGHT_FIELDS.put(FLD_SOURCE_AVAILABILITY, new String[]{FLD_SOURCE_AVAILABILITY_STORED});
      HIGHLIGHT_FIELDS.put(FLD_AUTHOR, new String[]{FLD_AUTHOR_STORED,FLD_TITLE_STORED});
   }

   // keep in sync with Util.NAMESPACE_MAP in wikidata project
   public static final String NS_PLACE_TEXT = "Place";
   public static final String NS_PERSON_TEXT = "Person";
   public static final String NS_SOURCE_TEXT = "Source";
   public static final String NS_ARTICLE_TEXT = "Article";
   public static final int NS_MAIN = 0;
   public static final int NS_USER = 2;
   public static final int NS_PROJECT = 4;
   public static final int NS_IMAGE = 6;
   public static final int NS_MEDIAWIKI = 8;
   public static final int NS_TEMPLATE = 10;
   public static final int NS_HELP = 12;
   public static final int NS_CATEGORY = 14;
   public static final int NS_GIVENNAME = 100;
   public static final int NS_SURNAME = 102;
   public static final int NS_SOURCE = 104;
   public static final int NS_PLACE = 106;
   public static final int NS_PERSON = 108;
   public static final int NS_FAMILY = 110;
   public static final int NS_MYSOURCE = 112;
   public static final int NS_REPOSITORY = 114;
   public static final int NS_PORTAL = 116;
   public static final int NS_TRANSCRIPT = 118;
   public static final Map<String,Integer> NAMESPACE_MAP = new HashMap<String,Integer>();
   static {
      NAMESPACE_MAP.put("Talk",NS_MAIN+1);
      NAMESPACE_MAP.put("User",NS_USER);
      NAMESPACE_MAP.put("User talk",NS_USER+1);
      NAMESPACE_MAP.put("WeRelate",NS_PROJECT);
      NAMESPACE_MAP.put("WeRelate talk",NS_PROJECT+1);
      NAMESPACE_MAP.put("Image",NS_IMAGE);
      NAMESPACE_MAP.put("Image talk",NS_IMAGE+1);
      NAMESPACE_MAP.put("MediaWiki",NS_MEDIAWIKI);
      NAMESPACE_MAP.put("MediaWiki talk",NS_MEDIAWIKI);
      NAMESPACE_MAP.put("Template",NS_TEMPLATE);
      NAMESPACE_MAP.put("Template talk",NS_TEMPLATE+1);
      NAMESPACE_MAP.put("Help",NS_HELP);
      NAMESPACE_MAP.put("Help talk",NS_HELP+1);
      NAMESPACE_MAP.put("Category",NS_CATEGORY);
      NAMESPACE_MAP.put("Category talk",NS_CATEGORY+1);
      NAMESPACE_MAP.put("Givenname",NS_GIVENNAME);
      NAMESPACE_MAP.put("Givenname talk",NS_GIVENNAME+1);
      NAMESPACE_MAP.put("Surname",NS_SURNAME);
      NAMESPACE_MAP.put("Surname talk",NS_SURNAME+1);
      NAMESPACE_MAP.put("Source",NS_SOURCE);
      NAMESPACE_MAP.put("Source talk",NS_SOURCE+1);
      NAMESPACE_MAP.put(NS_PLACE_TEXT,NS_PLACE);
      NAMESPACE_MAP.put("Place talk",NS_PLACE+1);
      NAMESPACE_MAP.put(NS_PERSON_TEXT,NS_PERSON);
      NAMESPACE_MAP.put("Person talk",NS_PERSON+1);
      NAMESPACE_MAP.put("Family",NS_FAMILY);
      NAMESPACE_MAP.put("Family talk",NS_FAMILY+1);
      NAMESPACE_MAP.put("MySource",NS_MYSOURCE);
      NAMESPACE_MAP.put("MySource talk",NS_MYSOURCE+1);
      NAMESPACE_MAP.put("Repository",NS_REPOSITORY);
      NAMESPACE_MAP.put("Repository talk",NS_REPOSITORY+1);
      NAMESPACE_MAP.put("Portal",NS_PORTAL);
      NAMESPACE_MAP.put("Portal talk",NS_PORTAL+1);
      NAMESPACE_MAP.put("Transcript",NS_TRANSCRIPT);
      NAMESPACE_MAP.put("Transcript talk",NS_TRANSCRIPT+1);
   }

   public static final String[] MAIN_NAMESPACES = new String[] {
      "Article", "User", "WeRelate", "Image", "MediaWiki", "Template", "Help", "Category", "Givenname", "Surname", "Source", NS_PLACE_TEXT,
      NS_PERSON_TEXT, "Family", "MySource", "Repository", "Portal", "Transcript"
   };

   public static final String[] MONTH_NAMES =
           {"january", "febuary", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"};

   private static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";

   private static Logger logger = Logger.getLogger("org.werelate.util");

   // Google romanizes the following
//   private static final char[] SPECIAL_CHARS =            {'´', 'ß',  'ј', 'ð', 'æ',  'ł', 'đ',  'ø',  'ŀ', 'і', 'þ',  'ı', 'œ',  'ĳ',
//                                                                      'Ј', 'Ð', 'Æ',  'Ł', 'Đ',  'Ø',  'Ŀ', 'І', 'Þ',       'Œ',  'Ĳ'};
//   private static final char[] SPECIAL_CHARS =              {180, 223, 1112, 240, 230,  322, 273,  248,  320,1110, 254,  305, 339,  307,
//                                                                       1032, 208, 198,  321, 272,  216,  319,1030, 222,       338,  306};
//   private static final String[] SPECIAL_TRANSLITERATIONS = {"'", "ss", "j", "d", "ae", "l", "dj", "oe", "l", "i", "th", "i", "oe", "y",
//                                                                        "J", "D", "Ae", "L", "Dj", "Oe", "L", "I", "Th",      "Oe", "Y"};

   private static final String[][] US_STATES_WITH_ABBREVS = {
      {"Alabama", "AL", "Ala"},
      {"Alaska", "AK", "Alak"},
      {"Arizona", "AZ", "Ariz"},
      {"Arkansas", "AR", "Ark"},
      {"California", "CA", "Cal", "Calif"},
      {"Colorado", "CO", "Colo", "Col"},
      {"Connecticut", "CT", "Conn"},
      {"Delaware", "DE", "Del"},
      {"District of Columbia", "DC", "D C"},
      {"Florida", "FL", "Fla"},
      {"Georgia", "GA"},
      {"Hawaii", "HI", "Haw"},
      {"Idaho", "ID", "Ida"},
      {"Illinois", "IL", "Ill"},
      {"Indiana", "IN", "Ind"},
      {"Iowa", "IA"},
      {"Kansas", "KS", "Kan"},
      {"Kentucky", "KY", "Ken"},
      {"Louisiana", "LA", "Louis"},
      {"Maine", "ME"},
      {"Maryland", "MD", "Mary"},
      {"Massachusetts", "MA", "Mass"},
      {"Michigan", "MI", "Mich"},
      {"Minnesota", "MN", "Minn"},
      {"Mississippi", "MS", "Miss"},
      {"Missouri", "MO"},
      {"Montana", "MT", "Mont"},
      {"Nebraska", "NE", "Neb"},
      {"Nevada", "NV", "Nev"},
      {"New Hampshire", "NH", "N H", "N Hamp", "N Hampshire"},
      {"New Jersey", "NJ", "N J", "N Jer", "N Jersey"},
      {"New Mexico", "NM", "N M", "N Mex", "N Mexico"},
      {"New York", "NY", "N Y", "N York"},
      {"North Carolina", "NC", "N C", "N Car", "N Carol", "N Carolina", "No Car", "No Carol", "No Carolina"},
      {"North Dakota", "ND", "N D", "N Dak", "N Dakota", "No Dak", "No Dakota"},
      {"Ohio", "OH"},
      {"Oklahoma", "OK", "Okl", "Okla"},
      {"Oregon", "OR", "Ore"},
      {"Pennsylvania", "PA", "Penn"},
      {"Rhode Island", "RI", "R I", "Rhode Is", "Rhode Isl", "R Is", "R Isl"},
      {"South Carolina", "SC", "S C", "S Car", "S Carol", "S Carolina", "So Car", "So Carol", "So Carolina"},
      {"South Dakota", "SD", "S D", "S Dak", "S Dakota", "So Dak", "So Dakota"},
      {"Tennessee", "TN", "Tenn"},
      {"Texas", "TX", "Tex"},
      {"Utah", "UT"},
      {"Vermont", "VT"},
      {"Virginia", "VA", "Vir", "Virg"},
      {"Washington", "WA", "Wash"},
      {"West Virginia", "WV", "WVa", "W V", "W Va", "W Vir", "W Virg"},
      {"Wisconsin", "WI", "Wisc", "Wis"},
      {"Wyoming", "WY", "Wyo"}
   };
   /**
    * Map US state abbreviations to the full state name.
    */
   private static Map<String,String> US_STATE_ABBREVS = new HashMap<String,String>();
   static {
      for (int i = 0; i < US_STATES_WITH_ABBREVS.length; i++) {
         for (int j = 1; j < US_STATES_WITH_ABBREVS[i].length; j++) {
            String abbrev = US_STATES_WITH_ABBREVS[i][j].toLowerCase();
            US_STATE_ABBREVS.put(abbrev, US_STATES_WITH_ABBREVS[i][0]);
         }
      }
   }

   public static String getUSStateFromAbbrev(String abbrev) {
      return US_STATE_ABBREVS.get(abbrev.replace(".","").replaceAll("\\s+", " ").trim().toLowerCase());
   }

   private static final String[] COUNTRY_ARRAY = {
     "Belize",
     "Canada",
     "Costa Rica",
     "El Salvador",
     "Guatemala",
     "Honduras",
     "Mexico",
     "Nicaragua",
     "Panama",
     "Saint-Pierre and Miquelon",
     "United States",
     "Albania",
     "Andorra",
     "Armenia",
     "Austria",
     "Azerbaijan",
     "Belarus",
     "Belgium",
     "Bosnia and Herzegovina",
     "Bulgaria",
     "Comoros",
     "Croatia",
     "Czech Republic",
     "Czechoslovakia",
     "Denmark",
     "England",
     "Estonia",
     "Faroe Islands",
     "Finland",
     "France",
     "Georgia (country)",
     "Germany",
     "Gibraltar",
     "Greece",
     "Greenland",
     "Guernsey",
     "Hungary",
     "Iceland",
     "Ireland",
     "Isle of Man",
     "Italy",
     "Latvia",
     "Liechtenstein",
     "Lithuania",
     "Luxembourg",
     "Macedonia",
     "Malta",
     "Moldova",
     "Monaco",
     "Netherlands",
     "Northern Ireland",
     "Norway",
     "Poland",
     "Portugal",
     "Republic of Ireland",
     "Romania",
     "San Marino",
     "Scotland",
     "Serbia and Montenegro",
     "Slovakia",
     "Slovenia",
     "Spain",
     "Svalbard",
     "Sweden",
     "Switzerland",
     "Ukraine",
     "Vatican City",
     "Wales",
     "Afghanistan",
     "Cyprus",
     "Bangladesh",
     "British Indian Ocean Territory",
     "Cambodia",
     "East Timor",
     "Iran",
     "Israel",
     "India",
     "Japan",
     "Jordan",
     "Kazakhstan",
     "Kuwait",
     "Kyrgyzstan",
     "Laos",
     "Lebanon",
     "Macau",
     "Malaysia",
     "Maldives",
     "Mongolia",
     "Myanmar",
     "Nepal",
     "North Korea",
     "Northern Cyprus",
     "Oman",
     "Pakistan",
     "Palestinian territories",
     "People's Republic of China",
     "Philippines",
     "Qatar",
     "Republic of China",
     "Russia",
     "Saudi Arabia",
     "Seychelles",
     "Singapore",
     "South Korea",
     "Soviet Union",
     "Sri Lanka",
     "Syria",
     "Tajikistan",
     "Thailand",
     "Turkey",
     "Turkmenistan",
     "United Arab Emirates",
     "Uzbekistan",
     "Vietnam",
     "Yemen",
     "Argentina",
     "Bolivia",
     "Brazil",
     "Chile",
     "Colombia",
     "Ecuador",
     "Falkland Islands",
     "French Guiana",
     "Guyana",
     "Paraguay",
     "Peru",
     "Uruguay",
     "Venezuela",
     "Anguilla",
     "Antigua and Barbuda",
     "Aruba",
     "Bahamas",
     "Barbados",
     "Bermuda",
     "British Virgin Islands",
     "Cayman Islands",
     "Cuba",
     "Dominica",
     "Dominican Republic",
     "Grenada",
     "Guadeloupe",
     "Haiti",
     "Jamaica",
     "Martinique",
     "Montserrat",
     "Netherlands Antilles",
     "Puerto Rico",
     "Saint Kitts and Nevis",
     "Saint Lucia",
     "Saint Vincent and the Grenadines",
     "Trinidad and Tobago",
     "Turks and Caicos Islands",
     "United States Virgin Islands",
     "Algeria",
     "Angola",
     "Bahrain",
     "Benin",
     "Bhutan",
     "Botswana",
     "Burkina Faso",
     "Burundi",
     "Cameroon",
     "Cape Verde",
     "Central African Republic",
     "Chad",
     "Côte d'Ivoire",
     "Democratic Republic of the Congo",
     "Djibouti",
     "Egypt",
     "Equatorial Guinea",
     "Eritrea",
     "Ethiopia",
     "Gabon",
     "Ghana",
     "Guinea",
     "Guinea-Bissau",
     "Kenya",
     "Lesotho",
     "Liberia",
     "Libya",
     "Madagascar",
     "Malawi",
     "Mali",
     "Morocco",
     "Mozambique",
     "Namibia",
     "Niger",
     "Nigeria",
     "Republic of the Congo",
     "Rwanda",
     "Saint Helena",
     "São Tomé and Príncipe",
     "Senegal",
     "Sierra Leone",
     "Somalia",
     "South Africa",
     "Sudan",
     "Suriname",
     "Swaziland",
     "Tanzania",
     "The Gambia",
     "Togo",
     "Tunisia",
     "Uganda",
     "Zambia",
     "Zimbabwe",
     "American Samoa",
     "Australia",
     "British Indian Ocean Territory",
     "Brunei",
     "Cook Islands",
     "Federated States of Micronesia",
     "Fiji",
     "French Polynesia",
     "Guam",
     "Indonesia",
     "Johnston Atoll",
     "Kiribati",
     "Marshall Islands",
     "Mauritania",
     "Mauritius",
     "Mayotte",
     "Midway Atoll",
     "Nauru",
     "New Caledonia",
     "New Zealand",
     "Niue",
     "Norfolk Island",
     "Northern Mariana Islands",
     "Palau",
     "Papua New Guinea",
     "Pitcairn Islands",
     "Réunion",
     "Samoa",
     "Solomon Islands",
     "Tokelau",
     "Tonga",
     "Tuvalu",
     "Vanuatu",
     "Wallis and Futuna"
   };
   public static final Set<String> COUNTRIES = new HashSet<String>(Arrays.asList(COUNTRY_ARRAY));

   /**
    * Convert non-roman but roman-like letters in the specified string to their roman (a-zA-Z) equivalents.
    * For example, strip accents from characters, and expand ligatures.
    * Adapted from Ancestry names code by Lee Jensen and Dallan Quass
    * @param s string to romanize
    * @return romanized word, may contain non-roman characters from non-roman-like alphabets like greek, arabic, hebrew
    */
   public static String romanize(String s) {
      if (s == null) {
         return "";
      }
      if (isAscii(s)) {
         return s;
      }

      StringBuilder buf = new StringBuilder();
      for (int i = 0; i < s.length(); i++) {
         char c = s.charAt(i);
         String replacement;
         if ((int)c > 127 && (replacement = CHARACTER_MAPPINGS.get(c)) != null) {
            buf.append(replacement);
         }
         else {
            buf.append(c);
         }
      }
      return buf.toString();
   }

   /**
    * Romanize the first inLen characters in the input buffer and store the array in output buffer
    * Adapted from Ancestry names code by Lee Jensen and Dallan Quass
    * @param in
    * @param inLen
    * @param out
    * @return length of result in output buffer; -1 if no substitutions were found
    */
   public static int romanize(char[] in, int inLen, char[] out) {
      if (in == null || inLen == 0) {
         return 0;
      }
      int outLen = 0;
      boolean match = false;

      for (int i = 0; i < inLen; i++) {
         char c = in[i];
         String replacement;
         if ((int)c > 127 && (replacement = CHARACTER_MAPPINGS.get(c)) != null) {
            replacement.getChars(0, replacement.length(), out, outLen);
            outLen += replacement.length();
            match = true;
         }
         else {
            out[outLen++] = c;
         }
      }
      return match ? outLen : -1;
   }

   public static String translateHtmlCharacterEntities(String in) {
      if (in == null) {
         return in;
      }
      StringBuffer buf = null;
      Matcher m = HTML_ENTITY_PATTERN.matcher(in);
      while (m.find()) {
         if (buf == null) {
            buf = new StringBuffer();
         }
         m.appendReplacement(buf, (String)HTML_ENTITY_MAP.get(m.group(1)));
      }
      if (buf == null) {
         return in;
      }
      else {
         m.appendTail(buf);
         return buf.toString();
      }
   }

   /**
    * Translate html character entities from input buffer to output buffer
    * @param in
    * @param out
    * @return false if no character entities were found; in which case output buffer is not modified
    */
   public static boolean translateHtmlCharacterEntities(StringBuilder in, StringBuffer out) {
      if (in == null || in.length() == 0) {
         return false;
      }
      boolean match = false;
      Matcher m = HTML_ENTITY_PATTERN.matcher(in);
      while (m.find()) {
         m.appendReplacement(out, (String)HTML_ENTITY_MAP.get(m.group(1)));
         match = true;
      }
      if (match) {
         m.appendTail(out);
      }
      return match;
   }

   public static String getMainNamespace(String namespace) {
      if (namespace.endsWith(" talk")) {
         return namespace.substring(0, namespace.length() - 5);
      }
      else if (isEmpty(namespace) || namespace.equals("Talk")) {
         return "Article";
      }
      return namespace;
   }

   public static long wikiTimestampToMillis(String ts) {
      GregorianCalendar gc = new GregorianCalendar(Integer.parseInt(ts.substring(0, 4)), Integer.parseInt(ts.substring(4, 6)) - 1,
                                                   Integer.parseInt(ts.substring(6, 8)), Integer.parseInt(ts.substring(8, 10)),
                                                   Integer.parseInt(ts.substring(10, 12)), Integer.parseInt(ts.substring(12, 14)));
      return gc.getTimeInMillis();
   }

   public static void sleep(int miliseconds) {
       try
       {
           Thread.sleep(miliseconds);
       } catch (InterruptedException e)
       {
           logger.warning(e.toString());
       }
   }

   public static Document parseText(Builder builder, String text, boolean addHeader) throws ParsingException, IOException {
      return builder.build(new StringReader((addHeader ? XML_HEADER  : "") + text));
   }

   /**
    * Returns true if the specified string contains only 7-bit ascii characters
    * @param in
    * @return boolean
    */
   public static boolean isAscii(String in) {
      for (int i = 0; i < in.length(); i++) {
         if (in.charAt(i) > 127) {
            return false;
         }
      }
      return true;
   }

   public static String[] splitNamespaceTitle(String fullTitle) {
      String[] fields = new String[2];
      fields[0] = "";
      fields[1] = fullTitle;

      int i = fullTitle.indexOf(":");
      if (i > 0) {
         String namespace = fullTitle.substring(0,i);
         Integer ns = Utils.NAMESPACE_MAP.get(namespace);
         if (ns != null) {
            fields[0] = namespace;
            fields[1] = fullTitle.substring(i+1);
         }
      }
      return fields;
   }

   /**
    * Returns the structured text in position 0 of the array, wiki text in position 1
    * @param text
    */
   public static String[] splitStructuredWikiText(String tagName, String text) {
      String[] split = new String[2];
      String endTag = "</" + tagName + ">";
      int pos = text.indexOf(endTag);
      if (pos >= 0) {
         pos += endTag.length();
         // skip over \n if present
         split[0] = text.substring(0, pos);
         split[1] = text.substring(pos);
      }
      else {
         split[0] = "";
         split[1] = text;
      }
      return split;
   }

   /**
    * Returns whether the specified string is null or has a zero length
    * @param s
    * @return boolean
    */
   public static boolean isEmpty(String s) {
      return (s == null || s.trim().length() == 0);
   }

   public static boolean termAttEquals(CharTermAttribute termAtt, String s) {
      if (termAtt.length() != s.length()) return false;
      char[] buffer = termAtt.buffer();
      for (int i = 0; i < s.length(); i++) {
         if (buffer[i] != s.charAt(i)) return false;
      }

      return true;
   }

   public static boolean termAttStartsWith(CharTermAttribute termAtt, String s) {
      if (termAtt.length() < s.length()) return false;

      char[] buffer = termAtt.buffer();
      for (int i = 0; i < s.length(); i++) {
         if (buffer[i] != s.charAt(i)) return false;
      }

      return true;
   }

   public static void setTermBuf(CharTermAttribute termAtt, char[] src, int srcLen) {
      char[] buffer = termAtt.buffer();
      if (buffer.length < srcLen) {
         buffer = termAtt.resizeBuffer(srcLen);
      }
      System.arraycopy(src, 0, buffer, 0, srcLen);
      termAtt.setLength(srcLen);
   }

   /**
    *
    * @param haystack - string to search
    * @param needle - string to look for - must be in lowercase
    * @return
    */
   public static boolean startsWithIgnoreCase(String haystack, String needle) {
      if (haystack.length() < needle.length()) {
         return false;
      }
      for (int i = 0; i < needle.length(); i++) {
         if (Character.toLowerCase(haystack.charAt(i)) != needle.charAt(i)) {
            return false;
         }
      }
      return true;
   }

   public static String toMixedCase(String s) {
      StringBuilder buf = new StringBuilder();
      boolean followsSpace = true;
      for (int i = 0; i < s.length(); i++) {
         char c = s.charAt(i);
         if (followsSpace) {
            buf.append(s.substring(i, i+1).toUpperCase()); // javadocs recommend this function instead of Character.toUpperCase(c)
         }
         else {
            buf.append(c);
         }
         followsSpace = (c == ' ');
      }
      return buf.toString();
   }

   public static String getWikiAjaxUrl(String hostname, String functionName, Map<String,String> args) {
      StringBuilder b = new StringBuilder();
      b.append("http://");
      b.append(hostname);
      b.append("/w/index.php?action=ajax&rs=");
      try
      {
         b.append(URLEncoder.encode(functionName, "UTF-8"));
         if (args.size() > 0) {
            b.append("&rsargs=");
         }
         boolean first = true;
         for (String k : args.keySet()) {
            String v = args.get(k);
            if (!first) {
               b.append("%7C");
            }
            first = false;
            b.append(URLEncoder.encode(k, "UTF-8"));
            b.append("=");
            b.append(URLEncoder.encode(v, "UTF-8"));
         }
      }
      catch (UnsupportedEncodingException e)
      {
         // ignore
      }
      return b.toString();
   }

   public static String getWikiAjaxUrl(String hostname, String functionName, Collection<String> args) {
      StringBuilder b = new StringBuilder();
      b.append("http://");
      b.append(hostname);
      b.append("/w/index.php?action=ajax&rs=");
      try
      {
         b.append(URLEncoder.encode(functionName, "UTF-8"));
         if (args.size() > 0) {
            b.append("&rsargs=");
         }
         boolean first = true;
         for (String arg : args) {
            if (!first) {
               b.append("%7C");
            }
            first = false;
            b.append(URLEncoder.encode(arg, "UTF-8"));
         }
      }
      catch (UnsupportedEncodingException e)
      {
         // ignore
      }
      return b.toString();
   }

   public static String join(String glue, Collection<String> strings) {
      StringBuilder b = new StringBuilder();
      boolean first = true;
      for (String s : strings) {
         if (!first) {
            b.append(glue);
         }
         first = false;
         b.append(s);
      }
      return b.toString();
   }

   public static String join(String glue, String[] strings) {
      StringBuilder b = new StringBuilder();
      boolean first = true;
      for (String s : strings) {
         if (!first) {
            b.append(glue);
         }
         first = false;
         b.append(s);
      }
      return b.toString();
   }

   public static void addQueryClause(Query out, Query q, BooleanClause.Occur occur) {
      if (out instanceof BooleanQuery) {
         ((BooleanQuery)out).add(q, occur);
      }
      else if (out instanceof DisjunctionMaxQuery) {
         ((DisjunctionMaxQuery)out).add(q);
      }
      else {
         throw new RuntimeException("Unexpected query type");
      }
   }

   /**
    * Return the number of occurrences of the specified character in the specified string
    */
   public static int countOccurrences(char ch, String in) {
      int cnt = 0;
      int pos = in.indexOf(ch);
      while (pos >= 0) {
         cnt++;
         pos = in.indexOf(ch, pos+1);
      }
      return cnt;
   }

   private static final String[] UPPERCASE_WORDS_ARRAY = {"I","II","III","IV","V","VI","VII","VIII","IX","X"};
   private static final Set<String> UPPERCASE_WORDS = new HashSet<String>();
   static {
      for (String word : UPPERCASE_WORDS_ARRAY) UPPERCASE_WORDS.add(word);
   }
   private static final String[] LOWERCASE_WORDS_ARRAY = {
      "a","an","and","at","but","by","for","from","in","into",
      "nor","of","on","or","over","the","to","upon","vs","with",
      "against", "as", "before", "between", "during", "under", "versus", "within", "through", "up",
      // french
      "à", "apres", "après", "avec", "contre", "dans", "dès", "devant", "dévant", "durant", "de", "avant", "des",
      "du", "et", "es", "jusque", "le", "les", "par", "passe", "passé", "pendant","pour", "pres", "près", "la",
      "sans", "suivant", "sur", "vers", "un", "une",
      // spanish
      "con", "depuis", "durante", "ante", "antes", "contra", "bajo",
      "en", "entre", "mediante", "para", "pero", "por", "sobre", "el", "o", "y",
      // dutch
      "aan", "als", "bij", "eer", "min", "na", "naar", "om", "op", "rond", "te", "ter", "tot", "uit", "voor",
      // german
      "auf", "gegenuber", "gegenüber", "gemäss", "gemass", "hinter", "neben",
      "über", "uber", "unter", "vor", "zwischen", "die", "das", "ein", "der",
      "ans", "aufs", "beim", "für", "fürs", "im", "ins", "vom", "zum", "am",
      // website extensions
      "com", "net", "org",
   };
   public static final Set<String> LOWERCASE_WORDS = new HashSet<String>();
   static {
      for (String word : LOWERCASE_WORDS_ARRAY) LOWERCASE_WORDS.add(word);
   }
   private static final String[] NAME_WORDS_ARRAY = {
      "a", "à", "contra", "das", "de", "der", "des", "die", "du", "ein", "el", "en", "la", "le", "les", "o", "sur", "te", "ter", "y",
   };
   private static final Set<String> NAME_WORDS = new HashSet<String>();
   static {
      for (String word : NAME_WORDS_ARRAY) NAME_WORDS.add(word);
   }
   public static final Pattern WORD_DELIM_REGEX = Pattern.compile("([ `~!@#$%&_=:;<>,./{}()?+*|\"\\-\\[\\]\\\\]+|[^ `~!@#$%&_=:;<>,./{}()?+*|\"\\-\\[\\]\\\\]+)");

   // keep in sync with wiki/StructuredData.captitalizeTitleCase
   public static String capitalizeTitleCase(String s) {
      StringBuilder result = new StringBuilder();
      boolean mustCap = true;
      Matcher m = WORD_DELIM_REGEX.matcher(s);
      while (m.find()) {
         String word = m.group(0);
         String ucWord = word.toUpperCase();
         String lcWord = word.toLowerCase();
         if (UPPERCASE_WORDS.contains(ucWord) ||    // upper -> upper
             word.equals(ucWord)) { // acronym or initial
            result.append(ucWord);
         }
         else if (NAME_WORDS.contains(lcWord) && !word.equals(lcWord)) {  // if word is a name-word entered mixed case, keep as-is
            result.append(word);
         }
         else if (!mustCap && LOWERCASE_WORDS.contains(lcWord)) { // upper/lower -> lower
            result.append(lcWord);
         }
         else if (word.equals(lcWord)) { // lower -> mixed
            result.append(word.substring(0,1).toUpperCase());
            result.append(word.substring(1).toLowerCase());
         }
         else { // mixed -> mixed
            result.append(word);
         }
         word = word.trim();
         mustCap = word.equals(":") || word.equals("?") || word.equals("!");
      }
      return result.toString();
   }

   public static String capitalizePlaceLevel(String placeName) {
      // lowercase everything within parentheses (township) (county); capitalize everything else (Parras de la Fuentes)
      int pos = placeName.indexOf('(');
      if (pos > 0) {
         placeName = capitalizeTitleCase(placeName.substring(0, pos).trim()) + " " + placeName.substring(pos).trim().toLowerCase();
      }
      else {
         placeName = capitalizeTitleCase(placeName.trim());
      }
      return placeName;
   }

   private static final Pattern UMLATS = Pattern.compile("[üöä]", Pattern.CASE_INSENSITIVE);

   // return a string with umlats converted to umlat-e, or null if no umlats found
   public static String convertUmlats(String s) {
      Matcher m = UMLATS.matcher(s);
      if (m.find()) {
         return s.replace("ü", "ue").replace("ö", "oe").replace("ä","ae").replace("Ü", "Ue").replace("Ö", "Oe").replace("Ä","Ae");
      }
      return null;
   }

   // Adapted from Ancestry names code by Lee Jensen and Dallan Quass
   private static final String[] CHARACTER_REPLACEMENTS = {
      "æ","ae",
      "ǝ","ae",
      "ǽ","ae",
      "ǣ","ae",
      "Æ","Ae",
      "Ə","Ae",
      "ß","ss",
      "đ","dj",
      "Đ","Dj",
      "ø","oe",
      "œ","oe",
      "Œ","Oe",
      "Ø","Oe",
      "þ","th",
      "Þ","Th",
      "ĳ","y",
      "Ĳ","Y",
      "á","a",
      "à","a",
      "â","a",
      "ä","a",
      "å","a",
      "ą","a",
      "ã","a",
      "ā","a",
      "ă","a",
      "ǎ","a",
      "ȃ","a",
      "ǻ","a",
      "ȁ","a",
      "Ƌ","a",
      "ƌ","a",
      "ȧ","a",
      "Ã","A",
      "Ą","A",
      "Á","A",
      "Ä","A",
      "Å","A",
      "À","A",
      "Â","A",
      "Ā","A",
      "Ă","A",
      "Ǻ","A",
      "ĉ","c",
      "ć","c",
      "č","c",
      "ç","c",
      "ċ","c",
      "Ĉ","C",
      "Č","C",
      "Ć","C",
      "Ç","C",
      "ð","d",
      "ď","d",
      "Ď","D",
      "Ð","D",
      "Ɖ","D",
      "ê","e",
      "é","e",
      "ë","e",
      "è","e",
      "ę","e",
      "ė","e",
      "ě","e",
      "ē","e",
      "ĕ","e",
      "ȅ","e",
      "Ė","E",
      "Ę","E",
      "Ê","E",
      "Ë","E",
      "É","E",
      "È","E",
      "Ě","E",
      "Ē","E",
      "Ĕ","E",
      "ƒ","f",
      "ſ","f",
      "ğ","g",
      "ģ","g",
      "ǧ","g",
      "ġ","g",
      "Ğ","G",
      "Ĝ","G",
      "Ģ","G",
      "Ġ","G",
      "Ɠ","G",
      "ĥ","h",
      "Ħ","H",
      "í","i",
      "і","i",
      "ī","i",
      "ı","i",
      "ï","i",
      "î","i",
      "ì","i",
      "ĭ","i",
      "ĩ","i",
      "ǐ","i",
      "į","i",
      "Í","I",
      "İ","I",
      "Î","I",
      "Ì","I",
      "Ï","I",
      "І","I",
      "Ĩ","I",
      "Ī","I",
      "ј","j",
      "ĵ","j",
      "Ј","J",
      "Ĵ","J",
      "ķ","k",
      "Ķ","K",
      "ĸ","K",
      "ł","l",
      "ŀ","l",
      "ľ","l",
      "ļ","l",
      "ĺ","l",
      "Ļ","L",
      "Ľ","L",
      "Ŀ","L",
      "Ĺ","L",
      "Ł","L",
      "ñ","n",
      "ņ","n",
      "ń","n",
      "ň","n",
      "ŋ","n",
      "ǹ","n",
      "Ň","N",
      "Ń","N",
      "Ñ","N",
      "Ŋ","N",
      "Ņ","N",
      "ô","o",
      "ö","o",
      "ò","o",
      "õ","o",
      "ó","o",
      "ő","o",
      "ơ","o",
      "ǒ","o",
      "ŏ","o",
      "ǿ","o",
      "ȍ","o",
      "ō","o",
      "ȯ","o",
      "ǫ","o",
      "Ó","O",
      "Ő","O",
      "Ô","O",
      "Ö","O",
      "Ò","O",
      "Õ","O",
      "Ŏ","O",
      "Ō","O",
      "Ơ","O",
      "Ƿ","P",
      "ƽ","q",
      "Ƽ","Q",
      "ř","r",
      "ŕ","r",
      "ŗ","r",
      "Ř","R",
      "Ʀ","R",
      "Ȓ","R",
      "Ŗ","R",
      "Ŕ","R",
      "š","s",
      "ś","s",
      "ş","s",
      "ŝ","s",
      "ș","s",
      "Ş","S",
      "Š","S",
      "Ś","S",
      "Ș","S",
      "Ŝ","S",
      "ť","t",
      "ţ","t",
      "ŧ","t",
      "ț","t",
      "Ť","T",
      "Ŧ","T",
      "Ţ","T",
      "Ț","T",
      "ũ","u",
      "ú","u",
      "ü","u",
      "ư","u",
      "û","u",
      "ů","u",
      "ù","u",
      "ű","u",
      "ū","u",
      "µ","u",
      "ǔ","u",
      "ŭ","u",
      "ȕ","u",
      "Ū","U",
      "Ű","U",
      "Ù","U",
      "Ú","U",
      "Ü","U",
      "Û","U",
      "Ũ","U",
      "Ư","U",
      "Ů","U",
      "Ǖ","U",
      "Ʊ","U",
      "ŵ","w",
      "Ŵ","W",
      "ÿ","y",
      "Ŷ","Y",
      "Ÿ","Y",
      "ý","y",
      "ȝ","y",
      "Ȝ","Y",
      "Ý","Y",
      "ž","z",
      "ź","z",
      "ż","z",
      "Ź","Z",
      "Ž","Z",
      "Ż","Z"
   };
   private static final HashMap<Character,String> CHARACTER_MAPPINGS = new HashMap<Character,String>();
   static {
      for (int i = 0; i < CHARACTER_REPLACEMENTS.length; i+=2) {
         CHARACTER_MAPPINGS.put(CHARACTER_REPLACEMENTS[i].charAt(0), CHARACTER_REPLACEMENTS[i+1]);
      }
   }

   private static final String[][] HTML_ENTITIES = {
      {"nbsp","32"},  // use normal space instead of hard space
      {"iexcl","161"},
      {"cent","162"},
      {"pound","163"},
      {"curren","164"},
      {"yen","165"},
      {"brvbar","166"},
      {"sect","167"},
      {"uml","168"},
      {"copy","169"},
      {"ordf","170"},
      {"laquo","171"},
      {"not","172"},
      {"shy","173"},
      {"reg","174"},
      {"macr","175"},
      {"deg","176"},
      {"plusmn","177"},
      {"sup2","178"},
      {"sup3","179"},
      {"acute","180"},
      {"micro","181"},
      {"para","182"},
      {"middot","183"},
      {"cedil","184"},
      {"sup1","185"},
      {"ordm","186"},
      {"raquo","187"},
      {"frac14","188"},
      {"frac12","189"},
      {"frac34","190"},
      {"iquest","191"},
      {"Agrave","192"},
      {"Aacute","193"},
      {"Acirc","194"},
      {"Atilde","195"},
      {"Auml","196"},
      {"Aring","197"},
      {"AElig","198"},
      {"Ccedil","199"},
      {"Egrave","200"},
      {"Eacute","201"},
      {"Ecirc","202"},
      {"Euml","203"},
      {"Igrave","204"},
      {"Iacute","205"},
      {"Icirc","206"},
      {"Iuml","207"},
      {"ETH","208"},
      {"Ntilde","209"},
      {"Ograve","210"},
      {"Oacute","211"},
      {"Ocirc","212"},
      {"Otilde","213"},
      {"Ouml","214"},
      {"times","215"},
      {"Oslash","216"},
      {"Ugrave","217"},
      {"Uacute","218"},
      {"Ucirc","219"},
      {"Uuml","220"},
      {"Yacute","221"},
      {"THORN","222"},
      {"szlig","223"},
      {"agrave","224"},
      {"aacute","225"},
      {"acirc","226"},
      {"atilde","227"},
      {"auml","228"},
      {"aring","229"},
      {"aelig","230"},
      {"ccedil","231"},
      {"egrave","232"},
      {"eacute","233"},
      {"ecirc","234"},
      {"euml","235"},
      {"igrave","236"},
      {"iacute","237"},
      {"icirc","238"},
      {"iuml","239"},
      {"eth","240"},
      {"ntilde","241"},
      {"ograve","242"},
      {"oacute","243"},
      {"ocirc","244"},
      {"otilde","245"},
      {"ouml","246"},
      {"divide","247"},
      {"oslash","248"},
      {"ugrave","249"},
      {"uacute","250"},
      {"ucirc","251"},
      {"uuml","252"},
      {"yacute","253"},
      {"thorn","254"},
      {"yuml","255"},
   };

   public static final Pattern HTML_ENTITY_PATTERN;
   static {
      StringBuffer buf = new StringBuffer();
      buf.append("&(");
      for (int i = 0; i < HTML_ENTITIES.length; i++) {
         if (i > 0) {
            buf.append("|");
         }
         buf.append(HTML_ENTITIES[i][0]);
      }
      buf.append(");");
      HTML_ENTITY_PATTERN = Pattern.compile(buf.toString());
   }

   public static final Map<String,String> HTML_ENTITY_MAP = new HashMap<String,String>();
   static {
      char[] chars = new char[1];
      for (int i = 0; i < HTML_ENTITIES.length; i++) {
         chars[0] = (char)Integer.parseInt(HTML_ENTITIES[i][1]);
         HTML_ENTITY_MAP.put(HTML_ENTITIES[i][0], new String(chars));
      }
   }

   private static final char[] HEX_CHARS = {'0', '1', '2', '3', '4', '5','6', '7', '8', '9', 'a', 'b','c', 'd', 'e', 'f'};

   public static String getMemcacheKey(String prefix, String s) {
      StringBuilder buf = new StringBuilder(32);
      try {
         MessageDigest md = MessageDigest.getInstance("MD5");
         byte[] digest = md.digest(s.getBytes());
         for (int i = 0; i < 16; i++) {
            int b = digest[i] & 0xff;
            buf.append(HEX_CHARS[b >> 4]);
            buf.append(HEX_CHARS[b & 0xf]);
         }
      } catch (NoSuchAlgorithmException e) {
         throw new RuntimeException("MD5 not found");
      }
      return prefix+buf.toString();
//      return s.replace(' ','_');
   }
}
