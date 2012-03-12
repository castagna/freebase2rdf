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

import java.io.IOException;
import java.io.OutputStream;

import org.openjena.atlas.lib.Sink;
import org.openjena.riot.out.EscapeStr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// "Lines are grouped by <source> and <property> and are ordered by a sort 
//  index when available, meaning all assertions about a particular topic 
//  with a particular relationship are contiguous and sorted roughly by importance." 
//  -- http://wiki.freebase.com/wiki/Data_dumps

public class Freebase2RDF implements Sink<String> {

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

    private final OutputStream out;
    private int count = 0;

    public Freebase2RDF ( OutputStream out ) {
        this.out = out;
    }

    @Override
    public void flush() {
        try { out.flush(); } catch (IOException e) { log.error(e.getMessage(), e); }
    }

    @Override
    public void close() {
        try { out.close(); } catch (IOException e) { log.error(e.getMessage(), e); }
    }

    @Override
    public void send ( String line ) {
    	try {
    		count++;
            String[] tokens = line.split("\\t");
            if ( tokens.length > 0 ) {
                if ( ( tokens.length == 3 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[2].trim().length() > 0) ) {            
                    resource ( tokens[0], tokens[1], tokens[2] );
                } else if ( ( tokens.length == 4 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[3].trim().length() > 0) ) {
                    if ( tokens[2].trim().length() == 0 ) {
                        literal ( tokens[0], tokens[1], tokens[3] );
                    } else {
                        if ( tokens[2].startsWith(LANG) ) {
                            literal_lang ( tokens[0], tokens[1], tokens[3], tokens[2].substring(tokens[2].lastIndexOf('/') + 1) );
                        } else {
                            if ( tokens[1].equals(OBJECT_KEY) ) {
                                literal2 ( tokens[0], tokens[1], tokens[2], tokens[3] );
                            } else if ( (tokens[1].equals(OBJECT_NAME)) && (tokens[2].startsWith(GUID)) ) {
                                literal2 ( tokens[0], tokens[1], tokens[2], tokens[3] );
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
    	} catch ( IOException e ) {
    		log.error(e.getMessage(), e);
    	}
    }
    
    private void resource ( String subject, String predicate, String object ) throws IOException {
        resource ( subject );
        resource ( predicate );
        resource ( object );
        dot ();
    }

    private void literal ( String subject, String predicate, String literal ) throws IOException {
        resource ( subject );
        resource ( predicate );
        literal ( literal );
        dot ();
    }

    private void literal2 ( String subject, String predicate1, String predicate2, String literal ) throws IOException {
        resource ( subject );
        resource ( predicate1, predicate2 );
        literal ( literal );
        dot ();
    }

    private void literal_lang ( String subject, String predicate, String literal, String lang ) throws IOException {
        resource ( subject );
        resource ( predicate );
        output_literal ( literal, lang );
        dot ();        
    }
    
    private void resource ( String resource ) throws IOException {
        out.write ( LT );
        out.write ( FREEBASE_NS );
        out.write ( resource.getBytes() );
        out.write ( GT );
        out.write ( SPACE );
    }

    private void resource ( String resource1, String resource2 ) throws IOException {
        out.write ( LT );
        out.write ( FREEBASE_NS );
        out.write ( resource1.getBytes() );
        out.write ( resource2.getBytes() );
        out.write ( GT );
        out.write ( SPACE );
    }

    private void literal ( String literal ) throws IOException {
        out.write ( DOUBLE_QUOTES );
        out.write ( EscapeStr.stringEsc( literal ).getBytes() );
        out.write ( DOUBLE_QUOTES );
        out.write ( SPACE );
    }
    
    private void output_literal ( String literal, String lang ) throws IOException {
        out.write ( DOUBLE_QUOTES );
        out.write ( EscapeStr.stringEsc( literal ).getBytes() );
        out.write ( DOUBLE_QUOTES );
        out.write ( AT );
        out.write ( lang.getBytes() );
        out.write ( SPACE );
    }
    
    private void dot () throws IOException {
        out.write ( DOT );
        out.write ( NL );
    }


}
