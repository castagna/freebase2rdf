  Freebase 2 RDF
  ==============

  Freebase, thanks Google, still publishes a dump of their data, here:
  http://download.freebase.com/datadumps/latest/
  
  Freebase2RDF is a small Java program which transform the Freebase data
  dump into RDF. The conversion is naive and no attempt is done to do
  clever tricks with literals (such as infer data types) nor extract a
  schema from the usage of 'properties'. (These are all possible 
  improvements, contributions welcome!)


  Requirements
  ------------

  The only requirements are a Java JDK 1.6 and Apache Maven.

  Instructions on how to install Maven are here:
  http://maven.apache.org/download.html#Installation 


  How to run it
  -------------

  First, download he Freebase latest data dump:
  wget http://download.freebase.com/datadumps/latest/freebase-datadump-quadruples.tsv.bz2

  cd freebase2rdf
  mvn package
  java -cp target/freebase2rdf-0.1-SNAPSHOT-jar-with-dependencies.jar com.kasabi.labs.freebase.Freebase2RDF </path/to/freebase-datadump-quadruples.tsv.bz2> </path/to/filename.nt.gz>
