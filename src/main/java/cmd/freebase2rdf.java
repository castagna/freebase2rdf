/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd;

import static com.hp.hpl.jena.sparql.util.Utils.nowAsString;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.openjena.atlas.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kasabi.labs.freebase.Freebase2RDF;

public class freebase2rdf {

    public static final Logger log = LoggerFactory.getLogger(freebase2rdf.class);

    public static void main(String[] args) throws Exception {
        if ( args.length != 2 ) { usage(); }
        File input = new File(args[0]);
        if ( !input.exists() ) error ("File " + input.getAbsolutePath() + " does not exist.");
        if ( !input.canRead() ) error ("Cannot read file " + input.getAbsolutePath());
        if ( !input.isFile() ) error ("Not a file " + input.getAbsolutePath());
        File output = new File(args[1]);
        if ( output.exists() ) error ("Output file " + output.getAbsolutePath() + " already exists, this program do not override existing files.");
        if ( output.canWrite() ) error ("Cannot write file " + output.getAbsolutePath());
        if ( output.isDirectory() ) error ("Not a file " + output.getAbsolutePath());
        if ( !output.getName().endsWith(".nt.gz") ) error ("Output filename should end with .nt.gz, this is the only format supported.");

        BufferedReader in = new BufferedReader ( new InputStreamReader ( new BZip2CompressorInputStream ( new FileInputStream ( input ) ) ) );
        BufferedOutputStream out = new BufferedOutputStream ( new GZIPOutputStream ( new FileOutputStream ( output ) ) );
        String line;

        Freebase2RDF freebase2rdf = new Freebase2RDF();
        ProgressLogger progressLogger = new ProgressLogger(log, "lines", 100000, 1000000);
        progressLogger.start();
        while ( ( line = in.readLine() ) != null ) {
        	freebase2rdf.sent(out, line);
            progressLogger.tick();

        }
        out.flush();
        out.close();

        print ( log, progressLogger );
    }

    private static void usage() {
        System.err.println("Usage: Freebase2RDF </path/to/freebase-datadump-quadruples.tsv.bz2> </path/to/filename.nt.gz>");
        System.exit(0);
    }

    private static void error ( String message ) {
        System.err.println(message);
        System.exit(0);
    }

    private static void print ( Logger log, ProgressLogger monitor ) {
        long time = monitor.finish() ;
        long total = monitor.getTicks() ;
        float elapsedSecs = time/1000F ;
        float rate = (elapsedSecs!=0) ? total/elapsedSecs : 0 ;
        String str =  String.format("Total: %,d lines : %,.2f seconds : %,.2f lines/sec [%s]", total, elapsedSecs, rate, nowAsString()) ;
        log.info(str) ;
    }
    
}
