/**
 * Copyright (C) 2008 Foundation for On-Line Genealogy (folg.org)
 */
package org.werelate.util;

import nu.xom.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WikiReader extends NodeFactory {
   private static Logger logger = Logger.getLogger("org.werelate.util");
   private static final Pattern REDIRECT_PATTERN = Pattern.compile("#redirect\\s*\\[\\[(.*?)\\]\\]", Pattern.CASE_INSENSITIVE);

   private Nodes EMPTY = new Nodes();
   private boolean inTitle;
   private boolean inText;
   private boolean inId;
   private boolean inRevision;
   private String title;
   private String text;
   private String id;
   private int latestId;
   private String latestText;
   private int cnt;
   private List<WikiPageParser> parsers;
   private boolean skipRedirects;

    public WikiReader() {
      parsers = new ArrayList<WikiPageParser>();
      inTitle = false;
      inText = false;
      inId = false;
      inRevision = false;
      skipRedirects = true;
   }

   public void setSkipRedirects(boolean skipRedirects) {
        this.skipRedirects = skipRedirects;
   }

   public Nodes makeComment(String data) {
       return EMPTY;
   }

   public Nodes makeText(String data) {
      if (inTitle) {
         title = data;
      }
      else if (inText) {
         text = data;
      }
      else if (inId) {
         id = data;
      }
      return EMPTY;
   }

   public Element makeRootElement(String name, String namespace) {
       return new Element(name, namespace);
   }

   public Nodes makeAttribute(String name, String namespace,
     String value, Attribute.Type type) {
       return EMPTY;
   }

   public Nodes makeDocType(String rootElementName,
     String publicID, String systemID) {
       return EMPTY;
   }

   public Nodes makeProcessingInstruction(
     String target, String data) {
       return EMPTY;
   }

   public Element startMakingElement(String name, String namespace) {
      boolean keep = false;
      if (name.equals("page")) {
         title = "";
         latestId = 0;
         latestText = "";
         keep = true;
      }
      else if (name.equals("title")) {
         inTitle = true;
         keep = true;
      }
      else if (name.equals("revision")) {
         inRevision = true;
         id = "";
         text = "";
         keep = true;
      }
      else if (inRevision && name.equals("id") && id.length() == 0) {  // ignore ID under page, and later id's under contributor
         inId = true;
         keep = true;
      }
      else if (inRevision && name.equals("text")) {
         inText = true;
         keep = true;
      }
      if (keep) {
         return super.startMakingElement(name, namespace);
      }
      return null;
   }

   public Nodes finishMakingElement(Element element) {
      if (element.getParent() instanceof Document) {
         return new Nodes(element);
      }
      String localName = element.getLocalName();
      if (localName.equals("revision")) {
         if (id.length() > 0) {
            try {
               int idNumber = Integer.parseInt(id);
               if (idNumber > latestId) {
                  latestId = idNumber;
                  latestText = text;
               }
               else {
                  logger.warning("IDs (" + latestId + " -> " + id + ") out of sequence for title: " + title);
               }
            }
            catch (NumberFormatException e) {
               logger.warning("Invalid ID: " + id + " for title: " + title);
            }
         }
         inRevision = false;
      }
      else if (localName.equals("page")) {
         if (++cnt % 100000 == 0) {
            System.out.print(".");
         }

         Matcher m = REDIRECT_PATTERN.matcher(latestText);
         if (title.length() == 0) {
            logger.warning("empty title");
         }
         else if (skipRedirects && m.lookingAt()) {
            // logger.info("skipping redirect: " + title);
         }
         else {
            for (WikiPageParser parser:parsers) {
               try {
                  parser.parse(title, latestText);
               } catch (IOException e) {
                  logger.severe("IOException: " + e);
               } catch (ParsingException e) {
                  logger.severe("Parsing exception for title: " + title + " - " + e);
               }
            }
         }
      }
      inTitle = false;
      inText = false;
      inId = false;
      return EMPTY;
   }

   public void addWikiPageParser(WikiPageParser parser) {
      parsers.add(parser);
   }

   public void removeWikiPageParser(WikiPageParser parser) {
      parsers.remove(parser);
   }

   public void read(String filename) throws ParsingException, IOException {
      InputStream in = new FileInputStream(filename);
      read(in);
      in.close();
   }

   public void read(InputStream in) throws ParsingException, IOException {
      title = null;
      cnt = 0;
      System.out.print("Indexing");
      Builder builder = new Builder(this);
      builder.build(in);
      System.out.println();
   }
}
