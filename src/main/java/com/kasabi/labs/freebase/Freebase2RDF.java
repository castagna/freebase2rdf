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

package com.kasabi.labs.freebase;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.openjena.atlas.logging.ProgressLogger;
import org.openjena.riot.out.EscapeStr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// "Lines are grouped by <source> and <property> and are ordered by a sort 
//  index when available, meaning all assertions about a particular topic 
//  with a particular relationship are contiguous and sorted roughly by importance." 
//  -- http://wiki.freebase.com/wiki/Data_dumps

public class Freebase2RDF {

    public static final Logger log = LoggerFactory.getLogger(Freebase2RDF.class);

    private static final byte[] FREEBASE_NS = "http://rdf.freebase.com/ns".getBytes();
    private static final String LANG = "/lang/";
    private static final String GUID = "/guid/";
    private static final String OBJECT_KEY = "/type/object/key";
    private static final String OBJECT_NAME = "/type/object/name";
    private static final byte[] LT = "<".getBytes();
    private static final byte[] GT = ">".getBytes();
    private static final byte[] DOT = ".".getBytes();
    private static final byte[] NL = "\n".getBytes();
    private static final byte[] AT = "@".getBytes();
    private static final byte[] SPACE = " ".getBytes();
    private static final byte[] DOUBLE_QUOTES = "\"".getBytes();
    
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
        int count = 0;
        ProgressLogger progressLogger = new ProgressLogger(log, "lines", 100000, 1000000);
        progressLogger.start();
        while ( ( line = in.readLine() ) != null ) {
            count++;
            progressLogger.tick();
            String[] tokens = line.split("\\t");
            if ( tokens.length > 0 ) {
                if ( ( tokens.length == 3 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[2].trim().length() > 0) ) {            
                    output_resource ( out, tokens[0], tokens[1], tokens[2] );
                } else if ( ( tokens.length == 4 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[3].trim().length() > 0) ) {
                    if ( tokens[2].trim().length() == 0 ) {
                        output_literal ( out, tokens[0], tokens[1], tokens[3] );
                    } else {
                        if ( tokens[2].startsWith(LANG) ) {
                            output_literal_lang ( out, tokens[0], tokens[1], tokens[3], tokens[2] );
                        } else {
                            if ( tokens[1].equals(OBJECT_KEY) ) {
                                output_literal2 ( out, tokens[0], tokens[1], tokens[2], tokens[3] );
                            } else if ( (tokens[1].equals(OBJECT_NAME)) && (tokens[2].startsWith(GUID)) ) {
                                output_literal2 ( out, tokens[0], tokens[1], tokens[2], tokens[3] );
                            } else {
                                log.warn ("Unexpected data at {}, ignoring: {}", count, line);
                            }
                        }
                    }           
                } else {
                    if ( tokens.length < 3 ) { 
                        log.warn ("Line {} has only {} tokens: {}", new Object[]{count, tokens.length, line});
                    } else {
                        log.warn ("Line {} has one or more empty tokens: {}", new Object[]{count, line});
                    }
                }
            
            }
        }
        
        progressLogger.finish();
    }

    private static void output_resource ( OutputStream out, String subject, String predicate, String object ) throws IOException {
        output_resource ( out, subject );
        output_resource ( out, predicate );
        output_resource ( out, object );
        output_dot ( out );
    }

    private static void output_literal ( OutputStream out, String subject, String predicate, String literal ) throws IOException {
        output_resource ( out, subject );
        output_resource ( out, predicate );
        output_literal ( out, literal );
        output_dot ( out );
    }

    private static void output_literal2 ( OutputStream out, String subject, String predicate1, String predicate2, String literal ) throws IOException {
        output_resource ( out, subject );
        output_resource ( out, predicate1, predicate2 );
        output_literal ( out, literal );
        output_dot ( out );
    }

    private static void output_literal_lang ( OutputStream out, String subject, String predicate, String literal, String lang ) throws IOException {
        output_resource ( out, subject );
        output_resource ( out, predicate );
        output_literal ( out, literal, lang );
        output_dot ( out );        
    }
    
    private static void output_resource ( OutputStream out, String resource ) throws IOException {
        out.write(LT);
        out.write(FREEBASE_NS);
        out.write(resource.getBytes());
        out.write(GT);
        out.write(SPACE);
    }

    private static void output_resource ( OutputStream out, String resource1, String resource2 ) throws IOException {
        out.write(LT);
        out.write(FREEBASE_NS);
        out.write(resource1.getBytes());
        out.write(resource2.getBytes());
        out.write(GT);
        out.write(SPACE);
    }

    private static void output_literal ( OutputStream out, String literal ) throws IOException {
        out.write ( DOUBLE_QUOTES );
        out.write ( EscapeStr.stringEsc( literal ).getBytes() );
        out.write ( DOUBLE_QUOTES );
        out.write(SPACE);
    }
    
    private static void output_literal ( OutputStream out, String literal, String lang ) throws IOException {
        out.write ( DOUBLE_QUOTES );
        out.write ( EscapeStr.stringEsc( literal ).getBytes() );
        out.write ( DOUBLE_QUOTES );
        out.write ( AT );
        out.write ( lang.getBytes() );
        out.write(SPACE);
    }
    
    private static void output_dot ( OutputStream out ) throws IOException {
        out.write ( DOT );
        out.write ( NL );
    }
    
    private static void usage() {
        System.err.println("Usage: Freebase2RDF </path/to/freebase-datadump-quadruples.tsv.bz2> </path/to/filename.nt.gz>");
        System.exit(0);
    }

    private static void error ( String message ) {
        System.err.println(message);
        System.exit(0);
    }

}
