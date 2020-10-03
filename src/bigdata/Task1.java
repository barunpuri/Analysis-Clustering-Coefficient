package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.function.IntPredicate;

import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Task1 extends Configured implements Tool{
	
	public static class T1Map extends Mapper<Object, Text, IntPairWritable, IntWritable>{
		
		IntWritable one = new IntWritable(1);
		
		@Override
		protected void map(Object key, Text value, 
				Mapper<Object, Text, IntPairWritable, IntWritable>.Context context) 
						throws IOException, InterruptedException{
			
			StringTokenizer st = new StringTokenizer(value.toString());
			int u = Integer.parseInt(st.nextToken());
			int v = Integer.parseInt(st.nextToken());
			
			if( u < v ) 
				context.write(new IntPairWritable(u, v), one);
			else if( u > v )
				context.write(new IntPairWritable(v, u), one);
			
			
		}	
	}
	
	public static class T1Reduce extends Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable>{
		
		@Override
		protected void reduce(IntPairWritable key, Iterable<IntWritable> values, 
				Reducer<IntPairWritable, IntWritable, IntWritable, IntWritable>.Context context) 
						throws IOException, InterruptedException{
			
			context.write(new IntWritable(key.u), new IntWritable(key.v));
		}
		
	}
	
	
	public static class T1Partitioner extends Partitioner<IntPairWritable, IntWritable>{

		@Override
		public int getPartition(IntPairWritable key, IntWritable value, int numPartitions) {
			return (key.u)%numPartitions;
		}
		
	}
	
	
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Task1(), args); 
    }
	
	public int run(String[] args) throws Exception{
		
		String input = args[0];
		String output = args[1];
		
		Job job = Job.getInstance(getConf());
		job.setJarByClass(Task1.class);
		
		job.setMapperClass(T1Map.class);
		job.setReducerClass(T1Reduce.class);
		
		job.setMapOutputKeyClass(IntPairWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setPartitionerClass(T1Partitioner.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.waitForCompletion(true);
		
		
		return 0;
	}
	
	
	
}
