/**
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

package com.kasabi.labs.freebase.mr;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.openjena.riot.out.EscapeStr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Freebase2RDFMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger log = LoggerFactory.getLogger(Freebase2RDFMapper.class);

    private static final byte[] FREEBASE_NS = "http://rdf.freebase.com/ns".getBytes();
    private static final String LANG = "/lang/";
    private static final String GUID = "/guid/";
    private static final String OBJECT_KEY = "/type/object/key";
    private static final String OBJECT_NAME = "/type/object/name";
    private static final byte[] LT = "<".getBytes();
    private static final byte[] GT = ">".getBytes();
    private static final byte[] DOT = ".".getBytes();
    private static final byte[] AT = "@".getBytes();
    private static final byte[] SPACE = " ".getBytes();
    private static final byte[] DOUBLE_QUOTES = "\"".getBytes();
    
    private Text k = new Text();
    private Text v = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        
    };
    
    @Override
    public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        log.debug("< ({}, {})", key, value);

        k.clear();
        v.clear();

        String[] tokens = value.toString().split("\\t");
        if ( tokens.length > 0 ) {
            if ( ( tokens.length == 3 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[2].trim().length() > 0) ) {            
                resource ( k, v, tokens[0], tokens[1], tokens[2] );
            } else if ( ( tokens.length == 4 ) && (tokens[0].trim().length() > 0) && (tokens[1].trim().length() > 0) && (tokens[3].trim().length() > 0) ) {
                if ( tokens[2].trim().length() == 0 ) {
                    literal ( k, v, tokens[0], tokens[1], tokens[3] );
                } else {
                    if ( tokens[2].startsWith(LANG) ) {
                        literal_lang ( k, v, tokens[0], tokens[1], tokens[3], tokens[2].substring(tokens[2].lastIndexOf('/') + 1) );
                    } else {
                        if ( tokens[1].equals(OBJECT_KEY) ) {
                            literal2 ( k, v, tokens[0], tokens[1], tokens[2], tokens[3] );
                        } else if ( (tokens[1].equals(OBJECT_NAME)) && (tokens[2].startsWith(GUID)) ) {
                            literal2 ( k, v, tokens[0], tokens[1], tokens[2], tokens[3] );
                        } else {
                            log.warn ("Unexpected data, ignoring: {}", value);
                        }
                    }
                }           
            } else {
                if ( tokens.length < 3 ) { 
                    log.warn ("Line with only {} tokens: {}", tokens.length, value.toString());
                } else {
                    log.warn ("Line with one or more empty tokens: {}", value.toString());
                }
            }
        }

        emit ( context, k, v );
    }

    private void emit ( Context context, Text key, Text value ) throws IOException, InterruptedException {
        context.write(key, value);
        log.debug("> ({}, {})", key, value);
    }

    private void resource ( Text key, Text value, String subject, String predicate, String object ) throws IOException {
        resource ( key, subject );
        resource ( value, predicate );
        resource ( value, object );
        dot ( value );
    }

    private void literal ( Text key, Text value, String subject, String predicate, String literal ) throws IOException {
        resource ( key, subject );
        resource ( value, predicate );
        literal ( value, literal );
        dot ( value );
    }

    private void literal2 ( Text key, Text value, String subject, String predicate1, String predicate2, String literal ) throws IOException {
        resource ( key, subject );
        resource ( value, predicate1, predicate2 );
        literal ( value, literal );
        dot ( value );
    }

    private void literal_lang ( Text key, Text value, String subject, String predicate, String literal, String lang ) throws IOException {
        resource ( key, subject );
        resource ( value, predicate );
        output_literal ( value, literal, lang );
        dot ( value );
    }

    private void append ( Text text, byte[] bytes ) {
        text.append ( bytes, 0, bytes.length );
    }

    private void append ( Text text, String str ) throws UnsupportedEncodingException {
        byte[] bytes = str.getBytes("UTF-8");
        text.append ( bytes, 0, bytes.length );
    }
    
    private void resource ( Text text, String resource ) throws IOException {
        append ( text, LT );
        append ( text, FREEBASE_NS );
        append ( text, resource );
        append ( text, GT );
        append ( text, SPACE );
    }

    private void resource ( Text text, String resource1, String resource2 ) throws IOException {
        append ( text, LT );
        append ( text, FREEBASE_NS );
        append ( text, resource1 );
        append ( text, resource2 );
        append ( text, GT );
        append ( text, SPACE );
    }

    private void literal ( Text text, String literal ) throws IOException {
        append ( text, DOUBLE_QUOTES );
        append ( text, EscapeStr.stringEsc( literal ) );
        append ( text, DOUBLE_QUOTES );
        append ( text, SPACE );
    }
    
    private void output_literal ( Text text, String literal, String lang ) throws IOException {
        append ( text, DOUBLE_QUOTES );
        append ( text, EscapeStr.stringEsc( literal ) );
        append ( text, DOUBLE_QUOTES );
        append ( text, AT );
        append ( text, lang );
        append ( text, SPACE );
    }
    
    private void dot ( Text text ) throws IOException {
        append ( text, DOT );
    }

}
