import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
public class step2 {
	/*
	 * The Input:
	 *      The output of step1
	 *
	 * The Output:
	 *      Lines with the same hook word will go to the same reducer.
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			Text text = new Text();
			text.set(String.format("%s",strings[0]));
			Text text1 = new Text();
			text1.set(String.format("%s",strings[1]));
			context.write(text ,text1);
		}
	}


	/*
	 * Input:
	 *      The input is the sorted output of the mapper 
	 *      Maybe output from different mappers.
	 *    
	 *
	 * Output:
	 *      hook	p(1,1);p(1,2);p(1,3);target1	p(2,1);p(2,2);p(2,3);target2	... 
	 *     
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text newKey = new Text();
			newKey.set(String.format("%s",key.toString()));
			String s="";
			for (Text val : values) {
				s=val.toString()+"\t"+s;
			}
			Text newVal = new Text();
			newVal.set(String.format("%s",s));
			context.write(newKey, newVal);

		}
	}

	private static class myPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}

	}
//save the output on the s3
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(step2.class);
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step2.myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//	String output="/output2/";
		//SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);

	}


}
