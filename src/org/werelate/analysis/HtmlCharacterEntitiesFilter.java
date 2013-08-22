package org.werelate.analysis;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.werelate.util.Utils;

import java.io.IOException;

/**
 * Created by Dallan Quass
 * Date: Apr 29, 2008
 */
public class HtmlCharacterEntitiesFilter extends TokenFilter
{
   private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
   private StringBuilder inBuf;
   private StringBuffer outBuf;

   public HtmlCharacterEntitiesFilter(TokenStream in) {
     super(in);
      inBuf = new StringBuilder();
      outBuf = new StringBuffer();
   }

   @Override
   public final boolean incrementToken() throws IOException {
      if (input.incrementToken()) {
        char[] buffer = termAtt.buffer();
        int length = termAtt.length();
        inBuf.setLength(0);
        inBuf.append(buffer, 0, length);
        outBuf.setLength(0);
        if (Utils.translateHtmlCharacterEntities(inBuf, outBuf)) {
           if (buffer.length < outBuf.length()) {
              buffer = termAtt.resizeBuffer(outBuf.length());
           }
           outBuf.getChars(0, outBuf.length(), buffer, 0);
           termAtt.setLength(outBuf.length());
        }
        return true;
     }
     return false;
   }
}
