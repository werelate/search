package org.werelate.search;

import org.apache.solr.handler.component.SearchComponent;
import org.folg.names.search.Normalizer;
import org.folg.names.search.Searcher;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.werelate.util.Utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class NameUpdater {
   // Keep in sync with the names project
   public static final String MEMCACHE_NAME_KEY_PREFIX = "name|";
   public static final String MEMCACHE_GIVENNAME_KEY_PREFIX = "g|";
   public static final String MEMCACHE_SURNAME_KEY_PREFIX = "s|";
   public static final int MEMCACHE_EXPIRATION = 86400;

   protected static Logger logger = Logger.getLogger("org.werelate.search");

   public static NameUpdater nameUpdater = new NameUpdater();

   public static NameUpdater getInstance() {
      return nameUpdater;
   }

   private static class DaemonBinaryConnectionFactory extends BinaryConnectionFactory {
      @Override
      public boolean isDaemon() {
         return true;
      }
   }

   private MemcachedClient memcachedClient;
   private String dbUrl;
   private String dbUser;
   private String dbPassword;

   public NameUpdater() {
      try {
         Properties properties = new Properties();
         properties.load(new FileInputStream("/etc/wr/java.properties"));
         String memcacheHost = properties.getProperty("memcache_host", "localhost");
         int memcachePort = Integer.parseInt(properties.getProperty("memcache_port", "11211"));
         memcachedClient = new MemcachedClient(new DaemonBinaryConnectionFactory(),
                                               AddrUtil.getAddresses(memcacheHost+":"+memcachePort));
      } catch (IOException e) {
         logger.severe("Unable to initialize memcache client: "+e.getMessage());
         throw new RuntimeException(e);
      }

      // read url, userName, password
      try {
         Properties userPassword = new Properties();
         userPassword.load(new FileInputStream("/etc/wr/java.properties"));
         dbUrl = userPassword.getProperty("db_url");
         dbUser = userPassword.getProperty("db_username");
         dbPassword = userPassword.getProperty("db_passwd");
      } catch (IOException e) {
         logger.severe("Unable to read /etc/wr/java.properties: "+e.getMessage());
         throw new RuntimeException(e);
      }

      try {
         Class.forName("com.mysql.jdbc.Driver").newInstance();
      } catch (Exception e) {
         logger.severe("Unable to find database driver: "+e.getMessage());
         throw new RuntimeException(e);
      }
   }

   private void updateName(PreparedStatement similarStatement, PreparedStatement logStatement, String userName, String tablePrefix,
                           Searcher searcher, String memcacheKeyPrefix, String namePiece,
                           Collection<String> adds, Collection<String> deletes, int flags, String comment) throws SQLException {
      // read current variants
      Searcher.ConfirmedComputerVariants ccVariants = searcher.getConfirmedComputerVariants(namePiece);
      Set<String> confirmedVariants = new TreeSet<String>(Arrays.asList(ccVariants.confirmedVariants));
      Set<String> computerVariants = new TreeSet<String>(Arrays.asList(ccVariants.computerVariants));

      boolean changed = false;

      // add new names
      changed = confirmedVariants.addAll(adds) || changed;
      changed = computerVariants.removeAll(adds) || changed;

      // remove names
      changed = confirmedVariants.removeAll(deletes) || changed;
      changed = computerVariants.removeAll(deletes) || changed;

      if (changed) {
         // insert or update db
         similarStatement.setString(1, namePiece);
         similarStatement.setString(2, Utils.join(" ", confirmedVariants));
         similarStatement.setString(3, Utils.join(" ", computerVariants));
         similarStatement.executeUpdate();

         // set memcache
         ccVariants = new Searcher.ConfirmedComputerVariants(confirmedVariants.toArray(new String[0]),
                                                             computerVariants.toArray(new String[0]));
         memcachedClient.set(memcacheKeyPrefix+namePiece, MEMCACHE_EXPIRATION, ccVariants);

         // log
         String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime());
         logStatement.setString(1, timestamp);
         logStatement.setString(2, userName);
         logStatement.setString(3, namePiece);
         logStatement.setString(4, tablePrefix);
         logStatement.setString(5, Utils.join(" ", adds));
         logStatement.setString(6, Utils.join(" ", deletes));
         logStatement.setInt(7, flags);
         logStatement.setString(8, comment);
         logStatement.executeUpdate();
      }
   }

   @SuppressWarnings("unchecked")
   public Collection<String> update(String name, boolean isSurname, String userName, String[] adds, String[] deletes, String comment, boolean isAdmin) {
      Collection<String> soundexNames = new HashSet<String>();

      Searcher searcher;
      String tablePrefix;
      String memcacheKeyPrefix;
      if (isSurname) {
         searcher = Searcher.getSurnameInstance();
         tablePrefix = "surname";
         memcacheKeyPrefix = MEMCACHE_NAME_KEY_PREFIX+MEMCACHE_SURNAME_KEY_PREFIX;
      }
      else {
         searcher = Searcher.getGivennameInstance();
         tablePrefix = "givenname";
         memcacheKeyPrefix = MEMCACHE_NAME_KEY_PREFIX+MEMCACHE_GIVENNAME_KEY_PREFIX;
      }

      // ensure comment is not null
      if (comment == null) {
         comment = "";
      }

      // ensure name is normalized
      Normalizer normalizer = Normalizer.getInstance();
      List<String> namePieces = normalizer.normalize(name, isSurname);
      if (namePieces == null || namePieces.size() != 1 || !namePieces.get(0).equals(name)) {
         return null;
      }

      // get current similar names
      Searcher.ConfirmedComputerVariants ccVariants = searcher.getConfirmedComputerVariants(name);
      Set<String> confirmedVariants = new TreeSet<String>(Arrays.asList(ccVariants.confirmedVariants));
      Set<String> computerVariants = new TreeSet<String>(Arrays.asList(ccVariants.computerVariants));

      // calculate delta from old to new
      Collection<String> trueAdds = new TreeSet<String>();
      Collection<String> trueDeletes = new TreeSet<String>();

      // process adds
      String code = searcher.getCode(name);
      for (String add : adds) {
         // remove rare names with same soundex code from adds and report them
         List<String> normalizedNames = normalizer.normalize(add, isSurname);
         if (normalizedNames.size() == 1) {
            add = normalizedNames.get(0);
            if (!add.equals(name)) {
               if (!searcher.isCommon(add) && searcher.getCode(add).equals(code)) {
                  soundexNames.add(add);
               }
               else if (!confirmedVariants.contains(add)) {
                  trueAdds.add(add);
               }
            }
         }
      }

      // process deletes
      for (String delete : deletes) {
         if (((isAdmin && confirmedVariants.contains(delete)) || computerVariants.contains(delete)) && !trueAdds.contains(delete)) {
            trueDeletes.add(delete);
         }
      }

      // update the database
      Connection conn = null;
      PreparedStatement similarStatement = null;
      PreparedStatement logStatement = null;
      if (trueAdds.size() > 0 || trueDeletes.size() > 0) {
         try {
            // connect to database and set up prepared statements
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            String sql = "insert into "+tablePrefix+"_similar_names (name, confirmed_variants, computer_variants) values(?,?,?) on duplicate key update confirmed_variants=values(confirmed_variants), computer_variants=values(computer_variants)";
            similarStatement = conn.prepareStatement(sql);
            sql = "insert into names_log (log_timestamp, log_user_text, log_name, log_type, log_adds, log_deletes, log_flags, log_comment) values (?,?,?,?,?,?,?,?)";
            logStatement = conn.prepareStatement(sql);

            // update variants for this name, and do the reverse update on all corresponding names
            updateName(similarStatement, logStatement, userName, tablePrefix, searcher, memcacheKeyPrefix,
                       name, trueAdds, trueDeletes, 0, comment);
            Collection<String> temp = new HashSet<String>();
            temp.add(name);
            for (String add : adds) {
               updateName(similarStatement, logStatement, userName, tablePrefix, searcher, memcacheKeyPrefix,
                          add, temp, Collections.EMPTY_SET, 1, comment);
            }
            for (String delete : deletes) {
               updateName(similarStatement, logStatement, userName, tablePrefix, searcher, memcacheKeyPrefix,
                          delete, Collections.EMPTY_SET, temp, 1, comment);
            }
         } catch (SQLException e) {
            logger.warning("Error writing to database: "+e.getMessage());
         }
         finally {
            try {
               if (similarStatement != null) {
                  similarStatement.close();
               }
               if (logStatement != null) {
                  logStatement.close();
               }
               if (conn != null) {
                  conn.close();
               }
            } catch (SQLException e) {
               // ignore
            }
         }
      }

      return soundexNames;
   }
}
