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

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

public class CsvToVectorMapper extends Mapper<LongWritable, Text, Text, VectorWritable> {
		
	private int columnNumber;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		columnNumber = Integer.parseInt(conf.get(DefaultOptionCreator.COLUMN_NUMBER));
	}
			
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
		
		// TODO: not always this problem
		// first line (column name), don't care
		if (key.get() == 0) {
			return;
		}
		int k = 0;
		double v = 0.0;
		long time=0;
		long start_time=1377605100;
		long end_time=1377618900;
		String keystring="";
		Vector input = new RandomAccessSparseVector(columnNumber);
		String[] values = line.toString().split(";");
		
		
		if(values.length > 4)
		    {
			keystring=values[0];
			long s_interval=start_time;
			long e_interval=start_time+300;
			
			long count_num=1;
			for(int i=1;i<values.length;i++)
			    {
				try{
				String[] time_value= values[i].split("=");
				time = Long.parseLong(time_value[0]);
				v += Double.parseDouble(time_value[1]);
				}
				catch (NumberFormatException e) {
				throw new IOException("CSV file contains non-numeric data");
				}
				if(time >= e_interval){
					input.setQuick(k,v/count_num);
					System.out.println(k+","+(v/count_num));
					k++;
					v=0;
					count_num=0;
					s_interval=e_interval;
					e_interval+=300;
				}
				count_num++;
			    }
			
			while(e_interval <= end_time){
				v=0;
				input.setQuick(k,v);
				System.out.println(k+","+(v/count_num));
				k++;
				e_interval+=300;
			}
		    }
				
		// Text type as key is required since "rowid" job takes as argument
		// SequenceFile<Text,VectorWritable>
		context.write(new Text(keystring), new VectorWritable(input));
	}
	
}
