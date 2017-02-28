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

public class Question1 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable> {

		 private final static NullWritable one = NullWritable.get();
		
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 	String line = value.toString();
			 	String[] tokens = line.split("::");
			 	
			 	if(tokens[1].contains("Palo Alto")) {
			 		tokens[2] = tokens[2].replace("List(", "");
			 		tokens[2] = tokens[2].replace(")", "");
			 		if(tokens[2]!= "") {
			 			context.write(new Text(tokens[2]), one);
			 		}
			 	}
		 }
	}
	
	public static class TokenReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
		HashSet<String> hs = new HashSet<String>();
		
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		   String cat = key.toString();
		   String[] categories = cat.split(",");
		 	for(String cat1 : categories) {
		 		cat1 = cat1.trim();
		 		if(!hs.contains(cat1)) {
		 			hs.add(cat1);
		 			context.write(new Text(cat1), NullWritable.get());
		 		}
		 		
		 	}
		 }
}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Question1");
	    job.setJarByClass(Question1.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(TokenReducer.class);
	    job.setMapOutputValueClass(NullWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
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
