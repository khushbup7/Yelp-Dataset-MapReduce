package Assignment1;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Question2 {
	
	public static class Question2_Mapper extends Mapper<Object, Text, Text, FloatWritable> {

		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 	String line = value.toString();
			 	String[] tokens = line.split("::");
			 	float rating = Float.parseFloat(tokens[3]);
			 	context.write(new Text(tokens[2]),new FloatWritable(rating));
			 }
	}
	
	public static class Question2_Reducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
		HashMap<String, Float> map = new HashMap<String, Float>();
		
		public void reduce(Text business_id, Iterable<FloatWritable> ratings, Context context) throws IOException, InterruptedException {
			int count = 0;
			float sum = 0;
			for(FloatWritable value : ratings) {
				sum += value.get();
				count ++;
			}
			float avg = sum/count;
			map.put(business_id.toString(), avg);
		}
		
		public void cleanup(Reducer<Text,FloatWritable,Text,FloatWritable>.Context context) throws IOException, InterruptedException{
			
			Map<String, Float> sortedMap = new TreeMap<String, Float>(new ValueComparator(map));
			sortedMap.putAll(map);

			int i = 0;
			for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
				context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
				i++;
				if (i == 10)
					break;
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Question2");
	    job.setJarByClass(Question2.class);
	    
	    job.setMapperClass(Question2_Mapper.class);
	    job.setReducerClass(Question2_Reducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(FloatWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    job.setNumReduceTasks(1);;
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(args[1]))){
			/*If exist delete the output path*/
			fs.delete(new Path(args[1]),true);
		}
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}

