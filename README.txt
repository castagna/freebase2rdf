  Freebase 2 RDF
  ==============

  Freebase, thanks to Google, still publishes a dump of their data, 
  here: http://download.freebase.com/datadumps/latest/
  
  Freebase2RDF is a small Java program which transform the Freebase 
  data dump into RDF. The conversion is naive and no attempt is made 
  to do clever stuff with literals (such as infer data types) nor 
  extract a schema from the usage of 'properties'. (These are all 
  possible improvements, contributions welcome!)


  Requirements
  ------------

  The only requirements are a Java JDK 1.6 and Apache Maven.

  Instructions on how to install Maven are here:
  http://maven.apache.org/download.html#Installation 


  How to run it
  -------------

  First, download the Freebase latest data dump:
  wget http://download.freebase.com/datadumps/latest/freebase-datadump-quadruples.tsv.bz2

    cd freebase2rdf
    mvn package
    java -cp target/freebase2rdf-0.1-SNAPSHOT-jar-with-dependencies.jar com.kasabi.labs.freebase.Freebase2RDF </path/to/freebase-datadump-quadruples.tsv.bz2> </path/to/filename.nt.gz>


  See also
  --------

   - http://code.google.com/p/freebase-quad-rdfize/
   - http://markmail.org/thread/mq6ylzdes6n7sc5o
   - http://markmail.org/thread/jegtn6vn7kb62zof


  MapReduce and how to use Apache Whirr
  -------------------------------------

  If you have an Hadoop cluster, here is how you can use 
  mvn hadoop:pack
  hadoop --config ~/.whirr/hadoop jar target/hadoop-deploy/freebase2rdf-hdeploy.jar cmd.freebase2rdf4mr </path/to/freebase-datadump-quadruples.tsv.bz2> </output/path>

  If you do not have an Hadoop cluster, here is how to use Apache Whirr:

    export KASABI_AWS_ACCESS_KEY_ID=...
    export KASABI_AWS_SECRET_ACCESS_KEY=...
    cd /opt/
    curl -O http://archive.apache.org/dist/whirr/whirr-0.7.1/whirr-0.7.1.tar.gz
    tar zxf whirr-0.7.1.tar.gz
    ssh-keygen -t rsa -P '' -f ~/.ssh/whirr
    export PATH=$PATH:/opt/whirr-0.7.1/bin/
    whirr version
    whirr launch-cluster --config hadoop-ec2.properties --private-key-file ~/.ssh/whirr
    . ~/.whirr/hadoop/hadoop-proxy.sh
    # Proxy PAC configuration here: http://apache-hadoop-ec2.s3.amazonaws.com/proxy.pac

  To shutdown the cluster:

    whirr destroy-cluster --config hadoop-ec2.properties

