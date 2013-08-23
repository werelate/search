Search
======

Search functionality for WeRelate.org

The search functionality is in two pieces: a customized SOLR search engine, and an indexing tool.

To build:
* create `conf/indexer.properties` from `conf/indexer.properties.sample` to match your environment
* create `conf/scripts.conf` from `conf/scripts.conf.sample` to match your environment
* run `ant build`

To run the customized SOLR search engine:
* set two environment variables in tomcat
    * `solr/home` points to the root directory of your index
    * `memcache_address` is the address of your memcache server; e.g., 127.0.0.1:11211
* copy the contents of the `dist` directory to the directory pointed to by solr/home
* copy `solr/apache-solr-3.1.0.war` to your webapps directory

To run the indexer
* run `bin/index.sh` periodically
