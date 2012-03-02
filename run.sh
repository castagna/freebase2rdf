#!/bin/sh

mvn clean package

# wget http://download.freebase.com/datadumps/latest/freebase-datadump-quadruples.tsv.bz2

java -cp target/freebase2rdf-0.1-SNAPSHOT-jar-with-dependencies.jar com.kasabi.labs.freebase.Freebase2RDF freebase-datadump-quadruples.tsv.bz2 freebase-datadump-quadruples.nt.gz
