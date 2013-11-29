/* Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.mahout.feature.common.csv;

import org.apache.mahout.feature.common.commandline.DefaultOptionCreator;

import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Map;


import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvToVectorDriver extends AbstractJob {
	
	private static final Logger log = LoggerFactory.getLogger(CsvToVectorDriver.class);
	
	public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CsvToVectorDriver(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
		
		addInputOption();
		addOutputOption();
		addOption(DefaultOptionCreator.columnNumberOption().create());
		addOption(DefaultOptionCreator.startTimeOption().create());
		addOption(DefaultOptionCreator.endTimeOption().create());
		addOption(DefaultOptionCreator.sloValueOption().create());
		Map<String,List<String>> parsedArgs = parseArguments(args);
		
		Path input = getInputPath();
		Path output = getOutputPath();
		
		int columnNumber = Integer.parseInt(getOption(DefaultOptionCreator.COLUMN_NUMBER));
		long start_time = new java.text.SimpleDateFormat("yyyy/MM/dd-HH:mm").parse(getOption(DefaultOptionCreator.START_TIME)).getTime() / 1000;
		long end_time = new java.text.SimpleDateFormat("yyyy/MM/dd-HH:mm").parse(getOption(DefaultOptionCreator.END_TIME)).getTime() / 1000;
		double slo_value = Double.parseDouble(getOption(DefaultOptionCreator.SLO_VALUE));
		Configuration confVector = getConf();
		confVector.set(DefaultOptionCreator.COLUMN_NUMBER, ""+columnNumber);
		confVector.set(DefaultOptionCreator.START_TIME, ""+start_time);
		confVector.set(DefaultOptionCreator.END_TIME, ""+end_time);
                confVector.set(DefaultOptionCreator.SLO_VALUE, ""+slo_value);
		Job jobVector = HadoopUtil.prepareJob(input,
                           output,
                           TextInputFormat.class,
                           CsvToVectorMapper.class,
                           Text.class,
                           VectorWritable.class,
                           SequenceFileOutputFormat.class,
                           confVector);
    jobVector.setJobName("Vectorizing Data");
    
    boolean succeededVector = jobVector.waitForCompletion(true);
		if (!succeededVector) return -1;
		
		return 0;
	}
	
}
