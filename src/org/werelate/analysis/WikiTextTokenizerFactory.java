package org.werelate.analysis;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.BaseTokenizerFactory;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceTokenizer;

import java.io.Reader;

/**
 * Created by Dallan Quass
 * Date: May 6, 2008
 */
public class WikiTextTokenizerFactory extends BaseTokenizerFactory {
  public Tokenizer create(Reader input) {
    return new WhitespaceTokenizer(Version.LUCENE_23, new WikiTextReader(input));
  }
}
