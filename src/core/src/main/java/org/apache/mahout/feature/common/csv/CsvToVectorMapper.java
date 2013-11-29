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
	long start_time;
	long end_time;
	double slo_value;
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		columnNumber = Integer.parseInt(conf.get(DefaultOptionCreator.COLUMN_NUMBER));
		start_time=Long.parseLong(conf.get(DefaultOptionCreator.START_TIME));
		end_time=Long.parseLong(conf.get(DefaultOptionCreator.END_TIME));
                slo_value=Double.parseDouble(conf.get(DefaultOptionCreator.SLO_VALUE));
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
                boolean classflag=false;
		String keystring="";
		Vector input = new RandomAccessSparseVector(columnNumber);
		String[] values = line.toString().split(";");
		
		
		if(values.length > 4)
		    {
			keystring=values[0];
                        classflag = keystring.equals("class");
			long s_interval=start_time;
			long e_interval=start_time+300;
			
			long count_num=1;
			for(int i=1;i<values.length;i++)
			    {
				try{
				String[] time_value= values[i].split("=");
				time = Long.parseLong(time_value[0]);
					if(time >= start_time && time <= end_time){
						v += Double.parseDouble(time_value[1]);
					}
				}
				catch (NumberFormatException e) {
				throw new IOException("CSV file contains non-numeric data");
				}
				if(time >= e_interval && time <= end_time){
				    double agg_v = v/count_num;
				 // First line in csv file should be class metric(slo metric)
				// if aggregated value greater than slo value, set 1 as violation for interval 
				// otherwise set 0 for compliance 
				    if (classflag==true) {
					if (agg_v>slo_value){
					    input.setQuick(k,1);
					}	
					else{
					    input.setQuick(k,-1);
					}
				    }
				// if line is not class metric use aggregated value for interval  
				    else{
					 input.setQuick(k,agg_v);
				    }
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
				k++;
				e_interval+=300;
			}
		    }
				
		// Text type as key is required since "rowid" job takes as argument
		// SequenceFile<Text,VectorWritable>
		context.write(new Text(keystring), new VectorWritable(input));
	}
	
}
