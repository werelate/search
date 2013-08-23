#!/bin/bash
cd "$( dirname "${BASH_SOURCE[0]}" )"
java -Xmx256m -Dfile.encoding=UTF-8 -Dnet.spy.log.LoggerImpl=net.spy.memcached.compat.log.SunLogger -Djava.util.logging.config.file=../conf/logging.properties -classpath \
../classes:\
../conf:\
../lib/commons-cli-1.0.jar:\
../lib/xom-1.1b5.jar:\
../lib/apache-solr-solrj-3.1.0.jar:\
../lib/apache-solr-core-3.1.0.jar:\
../lib/lucene-core-3.1-SNAPSHOT.jar:\
../lib/lucene-highlighter-3.1-SNAPSHOT.jar:\
../lib/mysql-connector-java-5.0.4-bin.jar:\
../lib/commons-codec-1.4.jar:\
../lib/commons-logging.jar:\
../lib/commons-httpclient-3.1.jar:\
../lib/slf4j-api-1.5.5.jar:\
../lib/slf4j-jcl-1.5.5.jar:\
../lib/spymemcached-2.7.3.jar:\
../lib/concurrent.jar:\
../lib/jcs-1.3.jar:\
../lib/lucid-kstem.jar:\
../lib/icu4j_3_4.jar \
org.werelate.indexer.Indexer -p ../conf/indexer.properties
