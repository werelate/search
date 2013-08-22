package org.werelate.search;

import org.folg.names.score.SimilarNameGenerator;
import org.folg.names.search.Normalizer;
import org.folg.names.search.Searcher;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.StringEncoder;
import org.apache.commons.codec.language.Soundex;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.apache.solr.core.SolrResourceLoader;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

public class NameReader {
   public static final double SURNAME_NAME_THRESHOLD = -0.4;
   public static final double SURNAME_CLUSTER_THRESHOLD = -3.0;
   public static final double GIVENNAME_NAME_THRESHOLD = 1.3;
   public static final double GIVENNAME_CLUSTER_THRESHOLD = -2.0;
   public static final int MAX_NAMES = 60;
   private static final String CANDIDATES_CACHE_NAME = "nameCandidates";

   protected static Logger logger = Logger.getLogger("org.werelate.search");

   public static NameReader nameReader = new NameReader();

   public static NameReader getInstance() {
      return nameReader;
   }

   public class NameInfo {
      public String namePiece = null;
      public String[] confirmedVariants = null;
      public String[] computerVariants = null;
      public String[] candidateVariants = null;
      public String[] soundexExamples = null;
      public String basename = null;
      public Collection<String> prefixedNames = null;
   }

   private SimilarNameGenerator givennameSimilarNameGenerator;
   private SimilarNameGenerator surnameSimilarNameGenerator;
   private Map<String,String[]> givennameSoundexExamples;
   private Map<String,String[]> surnameSoundexExamples;
   private JCS candidatesCache;

   private NameReader() {
      givennameSimilarNameGenerator = new SimilarNameGenerator(false, true);
      surnameSimilarNameGenerator = new SimilarNameGenerator(true, true);

      Reader givennameSoundexExamplesReader = null;
      Reader surnameSoundexExamplesReader = null;

      try
      {
         candidatesCache = JCS.getInstance(CANDIDATES_CACHE_NAME);
      } catch (CacheException e)
      {
         throw new RuntimeException("Couldn't instantiate cache: " + e.getMessage());
      }
      try {
         String solrHome = SolrResourceLoader.locateSolrHome();
         givennameSoundexExamplesReader = new FileReader(solrHome+"conf/givennameSoundexExamples.txt");
         givennameSoundexExamples = readSoundexExamples(givennameSoundexExamplesReader);
         surnameSoundexExamplesReader = new FileReader(solrHome+"conf/surnameSoundexExamples.txt");
         surnameSoundexExamples = readSoundexExamples(surnameSoundexExamplesReader);
      }
      catch (IOException e) {
         throw new RuntimeException("Error reading file:" + e.getMessage());
      }
      finally {
         try {
            if (givennameSoundexExamplesReader != null) {
               givennameSoundexExamplesReader.close();
            }
            if (surnameSoundexExamplesReader != null) {
               surnameSoundexExamplesReader.close();
            }
         }
         catch (IOException e) {
            // ignore
         }
      }
   }

   private Map<String,String[]> readSoundexExamples(Reader reader) throws IOException {
      Map<String,String[]> examples = new HashMap<String, String[]>();
      BufferedReader bufReader = new BufferedReader(reader);
      String line;
      while ((line = bufReader.readLine()) != null) {
         // line is code: names
         String[] fields = line.split("[: ]+",2);
         String code = fields[0];
         String[] names = fields[1].split("[, ]+");
         examples.put(code, names);
      }

      return examples;
   }

   public NameInfo getNameInfo(String name, boolean isSurname) {
      Searcher searcher;
      SimilarNameGenerator similarNameGenerator;
      Map<String,String[]> soundexExamples;
      double nameThreshold;
      double clusterThreshold;
      if (isSurname) {
         searcher = Searcher.getSurnameInstance();
         similarNameGenerator = surnameSimilarNameGenerator;
         soundexExamples = surnameSoundexExamples;
         nameThreshold = SURNAME_NAME_THRESHOLD;
         clusterThreshold = SURNAME_CLUSTER_THRESHOLD;
      }
      else {
         searcher = Searcher.getGivennameInstance();
         similarNameGenerator = givennameSimilarNameGenerator;
         soundexExamples = givennameSoundexExamples;
         nameThreshold = GIVENNAME_NAME_THRESHOLD;
         clusterThreshold = GIVENNAME_CLUSTER_THRESHOLD;
      }

      NameInfo nameInfo = new NameInfo();

      // normalize name
      List<String> namePieces = Normalizer.getInstance().normalize(name, isSurname);
      if (namePieces != null && namePieces.size() > 0) {
         String namePiece = namePieces.get(0);
         nameInfo.namePiece = namePiece;

         // read similar names
         Searcher.ConfirmedComputerVariants ccVariants = searcher.getConfirmedComputerVariants(namePiece);
         nameInfo.confirmedVariants = ccVariants.confirmedVariants;
         nameInfo.computerVariants = ccVariants.computerVariants;

         // read candidate names
         String cacheKey = (isSurname ? "S|" : "G|")+namePiece;
         String[] candidateNames = (String[])candidatesCache.get(cacheKey);
         if (candidateNames == null) {
            // filter out confirmed and computer variants from candidate variant list
            Set<String> candidateVariants = new TreeSet<String>(Arrays.asList(similarNameGenerator.generateSimilarNames(namePiece, nameThreshold, clusterThreshold, MAX_NAMES)));
            List<String> confirmedVariantList = Arrays.asList(ccVariants.confirmedVariants);
            List<String> computerVariantList = Arrays.asList(ccVariants.computerVariants);
            Iterator<String> iter = candidateVariants.iterator();
            while (iter.hasNext()) {
               String candidateName = iter.next();
               if (confirmedVariantList.contains(candidateName) || computerVariantList.contains(candidateName)) {
                  iter.remove();
               }
            }
            candidateNames = candidateVariants.toArray(new String[0]);

            try
            {
               candidatesCache.put(cacheKey, candidateNames);
            } catch (CacheException e)
            {
               logger.warning("Couldn't add "+cacheKey+" to candidatesCache: " + e.getMessage());
            }
         }
         nameInfo.candidateVariants = candidateNames;

         // read soundex examples
         StringEncoder coder = new Soundex();
         try {
            String code = coder.encode(namePiece);
            nameInfo.soundexExamples = soundexExamples.get(code);
         } catch (EncoderException e) {
            // ignore
         }

         if (isSurname) {
            // return basename or prefixed names
            nameInfo.basename = searcher.getBasename(namePiece);
            if (nameInfo.basename == null) {
               nameInfo.prefixedNames = searcher.getPrefixedNames(namePiece);
            }
         }
      }

      return nameInfo;
   }
}
