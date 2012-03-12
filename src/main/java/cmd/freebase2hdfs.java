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

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class freebase2hdfs extends Configured implements Tool {

    private static final Logger log = LoggerFactory.getLogger(freebase2hdfs.class);
    private static final int BUFFER_SIZE = 8 * 1024 * 1024 ; // 1 MB

    public freebase2hdfs() {
        super();
        log.debug("constructed with no configuration.");
    }
    
    public freebase2hdfs(Configuration configuration) {
        super(configuration);
        log.debug("constructed with configuration.");
    }   
    
    @Override
    public int run(String[] args) throws Exception {
        String input = null;
        String output = null;
        if ( args.length == 1 ) {
            input = "http://download.freebase.com/datadumps/latest/freebase-datadump-quadruples.tsv.bz2";
            output = args[0];
        } else if ( args.length == 2 ) {
            input = args[0];
            output = args[1];
        } else {
            System.err.printf("Usage: %s [generic options] [<http://path/to/freebase/datadump>] <hdfs://path/to/destination>\n", getClass().getName());
            System.err.println("[<http://path/to/freebase/datadump>] is optional, it defaults to http://download.freebase.com/datadumps/latest/freebase-datadump-quadruples.tsv.bz2\n");
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;     
        }
        
        Configuration configuration = getConf();
        FileSystem fs = FileSystem.get(configuration);
        Path outputPath = new Path(output);
        InputStream in = new URL(input).openStream();
        FSDataOutputStream out = fs.create(outputPath);
        IOUtils.copyBytes(in, out, BUFFER_SIZE, true);

        return 0;
    }
    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new freebase2hdfs(), args);
        System.exit(exitCode);
    }

}
