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

import static com.kasabi.labs.freebase.Testfreebase2rdf.dump;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Before;
import org.junit.Test;
import org.openjena.atlas.lib.FileOps;

import cmd.freebase2rdf4mr;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

public class TestFreebase2RDF4MRLocal {

	private static String output = "target/output" ;
	
    @Before public void setup() {
    	if ( FileOps.exists(output) ) {
        	FileOps.clearDirectory(output) ;    		
    	} else {
        	FileOps.ensureDir(output);    		
    	}
    }
	
    @Test public void test() throws Exception {
        String input = "src/test/resources/freebase-datadump-quadruples-mini.tsv.bz2" ;
        String[] args = new String[] {
        		"-conf", "conf/hadoop-local.xml",  
                "-D", Constants.OPTION_OVERRIDE_OUTPUT + "=true", 
        		"-D", Constants.OPTION_USE_COMPRESSION + "=" + Constants.OPTION_USE_COMPRESSION_DEFAULT, 
        		"-D", Constants.OPTION_RUN_LOCAL + "=true",
                input, 
                output
        };
        assertEquals ( 0, ToolRunner.run(new freebase2rdf4mr(), args) );
        Model mExpected = ModelFactory.createDefaultModel().read(new GZIPInputStream(new FileInputStream("src/test/resources/freebase-datadump-rdf-mini-expected.nt.gz")), null, "N-TRIPLE");
        Model mOutput = ModelFactory.createDefaultModel().read(new FileInputStream(output + "/part-r-00000"), null, "N-TRIPLE");
        assertTrue ( dump(mExpected,mOutput), mExpected.isIsomorphicWith(mOutput) );
   }

}
