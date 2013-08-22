package org.werelate.analysis;

import org.apache.lucene.util.Version;
import org.apache.solr.common.ResourceLoader;
import org.apache.lucene.analysis.CharArraySet;

import java.util.*;
import java.util.logging.Logger;
import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: May 7, 2008
 */
public class WordlistManager
{
   private static Logger logger = Logger.getLogger("org.werelate.analysis");

   private static WordlistManager wm = null;

   public static WordlistManager getInstance() {
      if (wm == null) {
         wm = new WordlistManager();
      }
      return wm;
   }

   private Map<String,CharArraySet>lists;

   private WordlistManager() {
      lists = new HashMap<String,CharArraySet>();
   }

   public CharArraySet getWordlist(String filename) {
      return lists.get(filename);
   }

   public CharArraySet getWordlist(String filename, ResourceLoader loader) {
      addWordlist(filename, loader);
      return getWordlist(filename);
   }

   private CharArraySet readFile(String filename, ResourceLoader loader) {
      CharArraySet words = null;
      try {
         words = new CharArraySet(Version.LUCENE_23, loader.getLines(filename), false);
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      return words;
   }

   public boolean addWordlist(String filename, ResourceLoader loader) {
      if (lists.get(filename) == null) {
         lists.put(filename, readFile(filename, loader));
         return true;
      }
      return false;
   }
}
