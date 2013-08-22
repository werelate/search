package org.werelate.analysis;

import java.io.Reader;
import java.io.IOException;
import java.io.BufferedReader;

/**
 * Created by Dallan Quass
 * Date: May 5, 2008
 */
public class WikiTextReader extends Reader
{
   private static final int PROC_TEXT = 0;
   private static final int PROC_ELM = 1;
   private static final int PROC_ATTR = 2;

   private Reader in;
   private int state;
   private int markState;

   public WikiTextReader(Reader source) {
      super();
      this.in=source.markSupported() ? source : new BufferedReader(source);
      this.state = PROC_TEXT;
   }

   public boolean ready() throws IOException
   {
      return in.ready();
   }

   public void close() throws IOException
   {
      in.close();
   }

   public void mark(int readAheadLimit) throws IOException
   {
      in.mark(readAheadLimit);
      markState = state;
   }

   public boolean markSupported() {
      return in.markSupported();
   }

   public void reset() throws IOException
   {
      state = markState;
      in.reset();
   }

   private boolean isAlphaNumeric(int ch) {
     return (ch>='a' && ch<='z') || (ch>='A' && ch<='Z') || ch == '_' || (ch>='0' && ch<='9');
   }

   public int read() throws IOException
   {
      int c = in.read();
      while (true) {
         if (state == PROC_TEXT && c == '<') {
            // eat </?elm and jump to PROC_ELM
            c = in.read();
            while (isAlphaNumeric(c) || c == '/') {
               c = in.read();
            }
            state = PROC_ELM;
         }
         else if (state == PROC_ATTR && c == '"') {
            // eat " and jump to PROC_ELM
            c = in.read();
            state = PROC_ELM;
         }
         else if (state == PROC_ELM && isAlphaNumeric(c)) {
            // eat attr=" and jump to PROC_ATTR
            while (isAlphaNumeric(c)) {
               c = in.read();
            }
            if (c == '=') {
               c = in.read();
               if (c == '"') {
                  c = in.read();
                  state = PROC_ATTR;
               }
            }
         }
         else if (state == PROC_ELM && (c == '/' || c == '>')) {
            // eat /? *> and jump to PROC_TEXT
            while (c == '/' || c == ' ') {
               c = in.read();
            }
            if (c == '>') {
               c = ' ';
            }
            state = PROC_TEXT;
         }
         else {
            return c;
         }
      }
   }

   public int read(char cbuf[], int off, int len) throws IOException {
     int i=0;
     for (i=0; i<len; i++) {
       int ch = read();
       if (ch==-1) break;
       cbuf[off++] = (char)ch;
     }
     if (i==0) {
       if (len==0) return 0;
       return -1;
     }
     return i;
   }
}
