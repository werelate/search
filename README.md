Search
======

Search functionality for WeRelate.org

The search functionality is in two pieces: a customized SOLR search engine, and an indexing tool.

To build:
* create `conf/indexer.properties` from `conf/indexer.properties.sample` to match your environment
* create `conf/scripts.conf` from `conf/scripts.conf.sample` to match your environment
* run `ant build`

To run the customized SOLR search engine:
* Set several environment variables in tomcat
    * `memcache_address` is the address of your memcache server; e.g., 127.0.0.1:11211
    * `db_url` is a jdbc connection string
    * `db_user` is the database user name
    * `dbPassword` is the database password
* and set a property:
    * `-Dsolr.solr.home` will the root directory of your solr installation
* you may be able to set these in /etc/default/tomcat7 using something like
    * memcache_address=127.0.0.1:11211
    * db_url=jdbc:mysql://localhost:3306/wikidb
    * db_username=user
    * db_passwd=secret
    * JAVA_OPTS="-Djava.awt.headless=true -Xms1100m -Xmx1100m -Dsolr.solr.home=/mnt/index"
* Copy the contents of the `dist` directory to the directory pointed to by solr.solr.home
* Copy `solr/apache-solr-3.1.0.war` to your webapps directory

To run the indexer
* run `bin/index.sh` periodically

Note
----
* This search project depends upon the Names project, which is in the middle of being revised. If you want to use this
Search project in its current state, contact me directly at dallan werelate.org and I will walk you through it.
