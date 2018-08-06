

import java.io.*;
import java.text.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class country 
{
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "check sale of product in every country");	
									job.setJarByClass(country.class);
									job.setMapperClass(map.class);
		//job.setCombinerClass(FileReducer.class);
									job.setReducerClass(reduce.class);
									job.setOutputKeyClass(Text.class);
									job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static class map extends Mapper<Object, Text, Text, LongWritable> 
	{
	

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
			{
				Text k= new Text("kids");
				Text t= new Text("Teenagers");
				Text a= new Text("Adults");
				Text o= new Text ("Over-aged");
				int age=-1;
				
				
				String line = value.toString();

				String[] p=line.split(",");

								String x=p[7];
				

								try {
									age =Integer.parseInt(x);}
								catch(Exception e){
										 age=-1;
									}
						
										
								 
									
				
				

				 if (age>0 && age<=12) { 
				 											
								context.write( k, new LongWritable(1));    }

			 if (age>12 && age<=19) { 
				 											
								context.write( t, new LongWritable(1));    }

			if (age>18 && age<=58) { 
				 											
								context.write( a, new LongWritable(1));    }
			if (age>58) { 
				 											
								context.write( o, new LongWritable(1));    }										
			
				

					
				
			}
		}

	public static class reduce extends  Reducer<Text, LongWritable, Text, LongWritable> 
	{
		

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,InterruptedException 
		{
			long sum = 0;
			for (LongWritable val : values) 
			{
				sum += val.get();
			}
			
			
			
			context.write(key, new LongWritable(sum));
			
		}
	}

}
