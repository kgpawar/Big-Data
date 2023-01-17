import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;
                  
public class Fb_likes1
{
	// Mapper class -> output -> string, int
	public static class Fb_likesMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			StringTokenizer s =new StringTokenizer(line,",");
			s.nextToken();
			String status_type = s.nextToken();
			String status_published = s.nextToken();
			s.nextToken();
			s.nextToken();
			s.nextToken();
			String num_likes = s.nextToken();
			while(s.hasMoreTokens())
			{
				s.nextToken();
			}
			if(status_type.equals("video"))
			{
				if(status_published.startsWith("2") && status_published.contains("2018"))
				{
					context.write(new Text("Vedeos_Likes"), new IntWritable(Integer.parseInt(num_likes)));
				}
			}
		}
	}
	// Reducer class -> string, int
	public static class Fb_likesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			IntWritable average = new IntWritable();
			int sum=0;
			for(IntWritable num : values) {
				sum+=num.get();
			}
			average.set(sum);
			context.write(key, average);
			
		}
	}
	public static void main(String args[]) throws Exception
	{
		// create the object of Configuration class
		Configuration conf = new Configuration();
	
		//create the object of job class
		Job job = new Job(conf, "Fb_likes");
		
		// Set the data type of output key
		job.setMapOutputKeyClass(Text.class);
		
		// Set the data type of output value
		job.setMapOutputValueClass(IntWritable.class);
	
		// set the data type of output key
		job.setOutputKeyClass(Text.class);
	
		// Set the data type of output value
		job.setOutputValueClass(IntWritable.class);
	
		// Set the data format of output
		job.setOutputFormatClass(TextOutputFormat.class);
	
		//set the data format of input
		job.setInputFormatClass(TextInputFormat.class);
	
		//set the name of Mapper class
		job.setMapperClass(Fb_likesMapper.class);
	
		//set the name of Reducer class
		job.setReducerClass(Fb_likesReducer.class);
	
		//set the input files path fr0om 0th argument
		FileInputFormat.addInputPath(job, new Path(args[0]));
	
		//set the output files path from 1st arg
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		//Execute the job and wait for completion		
		job.waitForCompletion(true);
	}
}
