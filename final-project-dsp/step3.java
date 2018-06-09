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
public class step3 {
	/**
	 * The Input:
	 *      The output of step2
	 *
	 * The Output:
	 *      Lines with the same pattern will go to the same reducer.
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public static int counter=0;
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			for(int i=1; i<strings.length;i++){
				String[] patterns= strings[i].split(";");
				for(int k=0; k<patterns.length-1;k++){
					Text text = new Text();
					text.set(String.format("%s",patterns[k]));
					Text text1 = new Text();
					text1.set(String.format("%s;%s",strings[0],patterns[patterns.length-1]));
					context.write(text ,text1);		
				}
			}
		}
	}


	/*
	 * Input:
	 *      The input is the sorted output of the mapper 
	 *      Maybe output from different mappers.
	 *    
	 *
	 * Output:
	 *     we will get all the pattern that don't have only one corpus
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text newKey = new Text();
			newKey.set(String.format("%s",key.toString()));
			int counter=0;
			for (Text val : values) {
				counter++;
			}
			if(counter>1){
				Text newVal = new Text();
				newVal.set(String.format("%s"," "));
				context.write(newKey, newVal);

			}
		}
	}
	private static class myPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}

	}

	//save on s3
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(step3.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step3.myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//String output="/output3/";
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);

	}


}
