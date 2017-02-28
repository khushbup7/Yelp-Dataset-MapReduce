package Assignment1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Question4 {
	
	public static class Question4_Mapper extends Mapper<Object, Text, Text, Text> {
		Set<String> hs = new HashSet<String>();
		public void setup(Context context) throws IOException, InterruptedException {
			try {
				URI[] localPaths = context.getCacheFiles();
				readFile(localPaths[0]);
			}
			catch (Exception e) {
				System.out.println(e);
			}
		}
		
		public void readFile(URI uri) {
			try {
			    BufferedReader fis = new BufferedReader(new FileReader(new File(uri.getPath()).getName()));
			    String line;
			    while ((line = fis.readLine()) != null) {
			      if(line.contains("Stanford,")) {
			    	  String[] tokens = line.split("::");
			    	  hs.add(tokens[0]);
			      }
			    	  
			    }
			    fis.close();
			  } catch (IOException ioe) {
				  System.out.println(ioe);
			  }
		}
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 	String line = value.toString();
			 	String[] tokens = line.split("::");
			 	String rating = tokens[3];
			 	String business_id = tokens[2];
			 	String user_id = tokens[1];
			 	if(hs.contains(business_id)) {
			 		context.write(new Text(user_id.toString()),new Text(rating));
			 	}
			 	
			 }
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Question4");
	    job.setJarByClass(Question4.class);
	    
	    job.setMapperClass(Question4_Mapper.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    Path inputPath = new Path(args[0]);
	    Path cachePath = new Path(args[1]);
	    Path outputPath = new Path(args[2]);
	    
	    
	    FileInputFormat.addInputPath(job, inputPath);
	    
	    job.addCacheFile(cachePath.toUri());
	    
	    FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			/*If exist delete the output path*/
			fs.delete(outputPath,true);
		}
	    FileOutputFormat.setOutputPath(job, outputPath);
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
