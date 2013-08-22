package org.werelate.indexer;

import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.commons.cli.*;

import java.util.Properties;
import java.util.logging.Logger;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.File;

/**
 * Created by Dallan Quass
 * Date: Jun 12, 2008
 */
public class Optimizer
{
   private static final Logger logger = Logger.getLogger("org.werelate.wiki");

   private Properties properties;

   public Optimizer(Properties properties) {
      this.properties = properties;
   }

   private void optimize() throws IOException, SolrServerException
   {
      String indexUrl = properties.getProperty("index_url");
      SolrServer solr = new CommonsHttpSolrServer(indexUrl);
      solr.optimize();
   }

   public static void main(String[] args) throws ParseException, IOException, SolrServerException
   {
      Options opt = new Options();
      opt.addOption("p", true, "java .properties file");
      opt.addOption("h", false, "Print out help information");

      BasicParser parser = new BasicParser();
      CommandLine cl = parser.parse(opt, args);
      if (cl.hasOption("h") || !cl.hasOption("p"))
      {
          System.out.println("Optimize index on SOLR server.");
          HelpFormatter f = new HelpFormatter();
          f.printHelp("OptionsTip", opt);
      }
      // read parameters and calculate stop time
      Properties properties = new Properties();
      properties.load(new FileInputStream(cl.getOptionValue("p")));
      Optimizer optimizer = new Optimizer(properties);
      optimizer.optimize();
   }
}
