Search
======

Search functionality for WeRelate.org

The search functionality is in two pieces: a customized SOLR search engine, and an indexing tool.

To build:
* create `conf/indexer.properties` from `conf/indexer.properties.sample` to match your environment
* create `conf/scripts.conf` from `conf/scripts.conf.sample` to match your environment
* run `ant build`

To run the customized SOLR search engine:
* Set an environment variable in tomcat
    * `memcache_address` is the address of your memcache server; e.g., 127.0.0.1:11211
* and set a property:
    * `-Dsolr.solr.home` will the root directory of your solr installation
* you may be able to set both in /etc/default/tomcat7 using something like
    * memcache_address=127.0.0.1:11211
    * JAVA_OPTS="-Djava.awt.headless=true -Xms1100m -Xmx1100m -Dsolr.solr.home=/mnt/index"
* Copy the contents of the `dist` directory to the directory pointed to by solr.solr.home
* Copy `solr/apache-solr-3.1.0.war` to your webapps directory

To run the indexer
* run `bin/index.sh` periodically
