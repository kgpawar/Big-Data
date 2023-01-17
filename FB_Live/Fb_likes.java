import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Fb_likes
{
	public static class LogMapper extends MapReduceBase implements Mapper <Object,/*Input Key Value*/
										  Text,/*Input value Type*/
										  Text,/*Output Key Value*/
										  FloatWritable>/*Output value Type*/
	{
		//Map function
		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
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
					output.collect(new Text("Video Likes"), new IntWritable(Integer.parseInt(num_likes));
				}
			}
		}
	}
		//Reducer class
	public static class LogReducer extends MapReduceBase implements Reducer<Text, FloatWritable, Text, FloatWritable>
	{
		public void Reduce(Text key,Iterator <IntWritable> values, OutputCollector <Text, IntWritable> output,
				     Reporter reporter) throws IOException
		{
			int sum = 0;
			for(IntWritable num:values)
			{
				sum=sum+num.get();		
			}
			output.collect(key, new IntWritable(sum));
		}		     
	}
	//Main function
	public static void main(String args[]) throws Exception
	{
		JobConf conf = new JobConf(Fb_likes.class);
		conf.setJobName("Fb Likes");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(LogMapper.class);
		conf.setReducerClass(LogReducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}
