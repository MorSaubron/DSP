import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


public class stepCreateHooksTarget {

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public static Long c0=new  Long(0);
		private static final Double fc=5000.0;
		private static final Double fb=1.0;

		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String w1 = strings[0];
			Double occur = Double.parseDouble(strings[1]);
			long d=c0/1000000;
			if(occur>=fb*d&&occur<=fc*d){
				Text text = new Text();
				text.set(String.format("%s",w1));
				Text text1 = new Text();
				text1.set(String.format("%s",occur.toString()));
				context.write(text,text1);
			}

		}

		public void setup(Mapper.Context context) throws IOException {  
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			RemoteIterator<LocatedFileStatus> it=fileSystem.listFiles(new Path("/output0"),false);
			while(it.hasNext()){
				LocatedFileStatus fileStatus=it.next();
				if (fileStatus.getPath().getName().startsWith("part")){
					FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
					BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, "UTF-8"));
					String line=null;
					String[] ones;
					while ((line = reader.readLine()) != null){
						ones = line.split("\t");
						if(ones[0].equals("*")){
							c0=Long.parseLong(ones[1]);
						}
					
					}
					reader.close();
				}
			}
		}
	}



	private static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String w1 = key.toString();
			Double sum_occ = 0.0;
			for (Text val : values) {
				sum_occ += Double.parseDouble(val.toString());
			}
			
			Text newKey = new Text();
			newKey.set(String.format("%s",w1));
			Text newVal = new Text();
			newVal.set(String.format("%s",sum_occ.toString()));
			context.write(newKey, newVal);
		}


	}

	private static class PartitionerClass extends Partitioner<Text,Text> {
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}

	public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException  {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(stepCreateHooksTarget.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(stepCreateHooksTarget.PartitionerClass.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//SequenceFileInputFormat.addInputPath(job, new Path("/output0/"));
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("/output0/"));
		String output="/hooksAndTargets1/";
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

	}
}





