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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;

public class Utils {

    public static String toString(String[] args) {
	    StringBuffer sb = new StringBuffer();
	    sb.append("{");
	    for ( String arg : args ) sb.append(arg).append(", ");
	    if ( sb.length() > 2 ) sb.delete(sb.length()-2, sb.length());
	    sb.append("}");
	    return sb.toString();
	}

	public static void setReducers(Job job, Configuration configuration, Logger log) {
	    boolean runLocal = configuration.getBoolean(Constants.OPTION_RUN_LOCAL, Constants.OPTION_RUN_LOCAL_DEFAULT);
	    int num_reducers = configuration.getInt(Constants.OPTION_NUM_REDUCERS, Constants.OPTION_NUM_REDUCERS_DEFAULT);
	
	    if ( runLocal ) {
	    	if (log != null) log.debug("Setting number of reducers to {}", 1);
	        job.setNumReduceTasks(1);
	    } else {
    		job.setNumReduceTasks(num_reducers);
	    	if (log != null) log.debug("Setting number of reducers to {}", num_reducers);
	    }
	}

	public static void log(Job job, Logger log) throws ClassNotFoundException {
		log.debug ("{} -> {} ({}, {}) -> {}#{} ({}, {}) -> {}", 
			new Object[]{
				job.getInputFormatClass().getSimpleName(), job.getMapperClass().getSimpleName(), job.getMapOutputKeyClass().getSimpleName(), job.getMapOutputValueClass().getSimpleName(), 
				job.getReducerClass().getSimpleName(), job.getNumReduceTasks(), job.getOutputKeyClass().getSimpleName(), job.getOutputValueClass().getSimpleName(), job.getOutputFormatClass().getSimpleName()
			}
		);
		Path[] inputs = FileInputFormat.getInputPaths(job);
		Path output = FileOutputFormat.getOutputPath(job);
		log.debug("input: {}", inputs[0]);
		log.debug("output: {}", output);
	}

}
