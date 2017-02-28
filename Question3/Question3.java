package Assignment1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3 {
	
	public static class Reviews_Mapper extends Mapper<Object, Text, Text, FloatWritable> {

		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 	String line = value.toString();
			 	String[] tokens = line.split("::");
			 	float rating = Float.parseFloat(tokens[3]);
			 	context.write(new Text(tokens[2]),new FloatWritable(rating));
			 }
	}
	
	public static class Details_Mapper extends Mapper<Object, Text, Text, Text> {

		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 String line = value.toString();
			 String[] tokens = line.split("::");
			 String business_id = tokens[0];
			 String details = "Det:"+ "\t" + tokens[1] + "\t" + tokens[2];
			 context.write(new Text(business_id), new Text(details));
		 }
	}
	
	public static class Top10Reviews_Mapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		 	String line = value.toString();
		 	String[] tokens = line.split("\t");
		 	String rating = tokens[1];
		 	String business_id = tokens[0];
		 	context.write(new Text(business_id), new Text(rating));
		 }
	}
	
	public static class Reviews_Reducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
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
	
	public static class Join_Reducer extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text business_id, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String rating ="";
			String details = "";
			boolean flag = false;
			for(Text value : values) {
				//System.out.println(value);
				String data = value.toString();
				if(data.contains("Det:")) {
					data = data.replace("Det:", "");
					details = data;
				}
				else {
					rating = data;
					flag = true;
				}
				
			}
			
			if(!flag)
				return;
			String data =  rating + "\t" + details;
			context.write(new Text(business_id), new Text(data));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();

	    Job job1 = Job.getInstance(conf, "Question3a");
	    job1.setJarByClass(Question2.class);
	    
	    job1.setMapperClass(Reviews_Mapper.class);
	    job1.setReducerClass(Reviews_Reducer.class);

	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(FloatWritable.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(FloatWritable.class);
	    //job1.setOutputFormatClass(customOutputFormat.class);
	    job1.setNumReduceTasks(1);;
	    
	    Path inputReviewPath = new Path(otherArgs[0]);
	    Path inputDetailsPath = new Path(otherArgs[1]);
	    Path outputPath = new Path(otherArgs[2]);
	    Path intermediatePath = new Path(otherArgs[3]);
	    
	    FileInputFormat.addInputPath(job1, inputReviewPath);
	    FileOutputFormat.setOutputPath(job1, intermediatePath);
	    
	    job1.waitForCompletion(true);
	    
	    // --------------------------------------------------------------
	    
	    Job job2 = Job.getInstance(conf, "Question3b");
	    job2.setJarByClass(Question3.class);
	    MultipleInputs.addInputPath(job2, inputDetailsPath, TextInputFormat.class , Details_Mapper.class);
	    MultipleInputs.addInputPath(job2, intermediatePath, TextInputFormat.class, Top10Reviews_Mapper.class);
		
		
	    job2.setReducerClass(Join_Reducer.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    //job2.setOutputFormatClass(customOutputFormat.class);
	    FileInputFormat.setMinInputSplitSize(job2, 500000000);
	    
	    FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			/*If exist delete the output path*/
			fs.delete(outputPath,true);
		}
	    FileOutputFormat.setOutputPath(job2, outputPath);

	    job2.waitForCompletion(true);
	    
		fs.delete(intermediatePath, true);
	    
	  }
}
