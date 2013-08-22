package org.werelate.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.folg.names.search.Normalizer;
import org.werelate.util.Utils;

import java.io.Reader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Dallan Quass
 * Date: May 7, 2008
 */
public class NameTokenizer extends Tokenizer {
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
   private boolean isSurname;
   private Normalizer normalizer;

   public NameTokenizer(Reader input, boolean isSurname) {
      super(input);
      this.isSurname = isSurname;
      normalizer = Normalizer.getInstance();
   }

   private static final int IO_BUFFER_SIZE = 4096;
   private final char[] ioBuffer = new char[IO_BUFFER_SIZE];
   private List<String> tokens = null;
   private int tokenPosition = 0;
   private int finalOffset = 0;

   @Override
   public final boolean incrementToken() throws IOException {
      // if no tokens, read the input to get more
      if (tokens == null || tokenPosition >= tokens.size()) {
         int dataLen = input.read(ioBuffer);
         if (dataLen == -1) {
            return false;
         }
         String name = new String(ioBuffer, 0, dataLen);
         tokens = normalizer.normalize(name, isSurname);
         if (tokens == null || tokens.size() == 0) {
            // if we didn't get anything tokenized, index as-is
            tokens = new ArrayList<String>();
            for (String namePiece : name.split("\\s+")) {
               namePiece = Utils.romanize(namePiece.toLowerCase()).replaceAll("[^a-z]+", "");
               if (namePiece.length() > 0) {
                  tokens.add(namePiece);
               }
            }
         }
         if (tokens == null || tokens.size() == 0) {
            return false;
         }
         tokenPosition = 0;
         finalOffset = -1;
      }

      // return the next token
      String token = tokens.get(tokenPosition);
      tokenPosition++;
      char[] buffer = termAtt.buffer();
      if (token.length() >= buffer.length) {
         buffer = termAtt.resizeBuffer(token.length()+1);
      }
      System.arraycopy(token.toCharArray(), 0, buffer, 0, token.length());
      termAtt.setLength(token.length());
      // start is approximate, since I don't know how to determine actual token start
      int start = finalOffset + 1;
      finalOffset = start+token.length();
      offsetAtt.setOffset(start, finalOffset);
      return true;
   }

   @Override
   public final void end() {
     // set final offset
     offsetAtt.setOffset(finalOffset, finalOffset);
   }

   @Override
   public void reset(Reader input) throws IOException {
     super.reset(input);
     finalOffset = 0;
   }
}
