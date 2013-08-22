package org.werelate.util;

/**
 * Created by Dallan Quass
 * Date: Apr 28, 2008
 */
import nu.xom.ParsingException;

import java.io.IOException;

public interface WikiPageParser {
   public void parse(String title, String text) throws IOException, ParsingException;
}
