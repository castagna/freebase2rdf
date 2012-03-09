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

package com.kasabi.labs.freebase;

import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.zip.GZIPInputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.openjena.atlas.lib.FileOps;

import cmd.freebase2rdf;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

@RunWith(Parameterized.class)
public class Testfreebase2rdf {

	public static final Object[][] TEST_DATA = new Object[][] {
        { "src/test/resources/freebase-datadump-quadruples-mini.tsv.bz2", "src/test/resources/freebase-datadump-rdf-mini-expected.nt.gz", "target/freebase-datadump-rdf-mini.nt.gz" }, 
	};

    @Before public void setup() {
    	if ( FileOps.exists(output) ) {
    		FileOps.delete(output);
    	}
    }

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(TEST_DATA);
    }

    private String input ;
    private String expected ;
    private String output ;
    
    public Testfreebase2rdf ( String input, String expected, String output ) {
        this.input = input ;
        this.expected = expected ;
        this.output = output ;
    }
    
    @Test public void test() throws Exception { 
    	run (input, expected, output); 
    }
    
    private void run ( String input, String expected, String output ) throws Exception {
        String[] args = new String[] { input, output };
        freebase2rdf.main(args);
        Model mExpected = ModelFactory.createDefaultModel().read(new GZIPInputStream(new FileInputStream(expected)), null, "N-TRIPLE");
        Model mOutput = ModelFactory.createDefaultModel().read(new GZIPInputStream(new FileInputStream(output)), null, "N-TRIPLE");
        assertTrue ( dump(mExpected,mOutput), mExpected.isIsomorphicWith(mOutput) );
    }
    
    private String dump ( Model m1, Model m2 ) {
        StringBuffer sb = new StringBuffer();
        sb.append("\n");
        Model d12 = m1.difference(m2);
        if ( d12.size() > 0 ) {
            sb.append("Triples contained only in the expected model:\n");
            dump ( sb, m1.difference(m2) );        	
        }
        Model d21 = m2.difference(m1);
        if ( d21.size() > 0 ) {
            sb.append("Triples contained only in the output model:\n");
            dump ( sb, m2.difference(m1) );        	
        }
        return sb.toString();
    }

	private void dump ( StringBuffer sb, Model m ) {
		StmtIterator iter = m.listStatements();
		while ( iter.hasNext() ) {
			Statement stmt = iter.next();
			sb.append(stmt.toString());
			sb.append("\n");
		}
	}

}
