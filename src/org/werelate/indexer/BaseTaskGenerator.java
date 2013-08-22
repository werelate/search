package org.werelate.indexer;

import org.werelate.util.HttpClientHelper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.net.URLEncoder;

import nu.xom.ParsingException;

/**
 * Created by Dallan Quass
 * Date: Apr 18, 2008
 */
public abstract class BaseTaskGenerator
{
   protected static final Logger logger = Logger.getLogger("org.werelate.wiki");

   public abstract List<IndexTask> getTasks(CheckpointManager cm, int max) throws SQLException, IOException, ParsingException;
   public abstract void updateCheckpoint(IndexTask it, CheckpointManager cm);

   protected HttpClientHelper client;
   protected String hostname;

   protected BaseTaskGenerator(HttpClientHelper client, String hostname) {
      this.client = client;
      this.hostname = hostname;
   }

   private BaseTaskGenerator() {}

}
