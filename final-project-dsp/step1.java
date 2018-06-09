import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.amazonaws.services.inspector.model.StopAction;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;


public class step1 {
	/*
	 * The Input:
	 *      Google 5gram database after filtering
	 *      		T n-gram         /	T year / T occurrences
	 *              a babe in the woods	2011	16
	 *The output:
	 *	the key will be the second and the forth word
	 *	the value will be all the sentence
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String[] words = strings[0].split(" ");
			if(words.length>4){
				String w2 = words[1];
				String w4= words[3];
				Text text = new Text();
				text.set(String.format("%s %s",w2,w4));
				Text text1 = new Text();
				text1.set(String.format("%s",strings[0]));
				context.write(text ,text1);
			}

		}

	}

	/*
	 * Input:
	 *      The input is the sorted output of the mapper 
	 *      Maybe output from different mappers.
	 * create the patterns to every hook word.
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public static HashSet<String> hooktargetMap= new HashSet<String>(); 
		public static HashSet<String> hfw= new HashSet<String>(); 

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String [] oldkey=key.toString().split(" ");		
			if((hooktargetMap.contains(oldkey[0]))&& (hooktargetMap.contains(oldkey[1]))){
				Text newKey = new Text();
				newKey.set(String.format("%s",oldkey[0]));
				Text newVal = new Text();
				String s="";
				for (Text val : values) {
					String [] v=val.toString().split(" ");
					String w1=v[0];
					String w3= v[2];
					String w5= v[4];
					if(hfw.contains(w1)&&(hfw.contains(w3))&&(hfw.contains(w5))){
						s=s+w1+" X "+w3+" Y "+w5+";";
					}
				}				
				if(!s.isEmpty()){
					newVal.set(String.format("%s%s",s,oldkey[1]));
					context.write(newKey, newVal);
				}
			}

		}
		//we will get all the hook-target corpus and the HFW corpus 
		public void setup(Reducer.Context context) throws IOException {  
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			RemoteIterator<LocatedFileStatus> it=fileSystem.listFiles(new Path("/hooksAndTargets1"),false);
			List<String> l=new LinkedList<String>(); 
			int counter=0;
			while(it.hasNext()){
				LocatedFileStatus fileStatus=it.next();
				if (fileStatus.getPath().getName().startsWith("part")){
					FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
					BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, "UTF-8"));
					String line=null;
					String[] ones;
					while ((line = reader.readLine()) != null){
						ones = line.split("\t");
						if(counter%5==0){
							hooktargetMap.add(ones[0].toString());
						}
						counter++;
					}

					reader.close();
				}
			}

			it=fileSystem.listFiles(new Path("/HFW1"),false);
			while(it.hasNext()){
				LocatedFileStatus fileStatus=it.next();
				if (fileStatus.getPath().getName().startsWith("part")){
					FSDataInputStream InputStream = fileSystem.open(fileStatus.getPath());
					BufferedReader reader = new BufferedReader(new InputStreamReader(InputStream, "UTF-8"));
					String line=null;
					String[] ones;
					while ((line = reader.readLine()) != null){
						ones = line.split("\t");
						hfw.add(ones[0].toString());
					}

					reader.close();
				}
			} 
		}
	}

	private static class myPartitioner extends Partitioner<Text, Text>{
		@Override
		public int getPartition(Text key, Text value, int numPartitions){
			return Math.abs(key.hashCode()) % numPartitions;
		}
	}




	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(step1.class);
		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step1.myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		//job.setInputFormatClass(SequenceFileInputFormat.class);
		//SequenceFileInputFormat.addInputPath(job, new Path("/smallCorpus/"));
		//String output="/output11/";
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		job.waitForCompletion(true);


	}

}
