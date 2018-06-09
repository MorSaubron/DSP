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

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class create20precentCorpus{

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public static final String[] STOP_WORDS = {"\";","\":","\".","\"!","\"?","\"\"","\"","׳","־","^","?",";",":","1",".",",","-","*","!",")","(","•","״","c","d","e","f","g","h","j","k","l","m","n","o","p","q","r","s","t","v","w","x","y","z","b","—","0","1","2","3","4","5","6","7","8","9","�","^","?",";",":","."
				,"|","£","¥","§","©","«","®","°","±","»","être","¿","à","B","C","D","E","F","■","O","<","₪","-","—","*","!","(",")","'","@","#","$","\"","%","&","[","]","{","}","-","/","+","=",".","'",",","~","�"};
		public int counter=0;
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String[] words = strings[0].split(" ");
			List<String> stopWords = Arrays.asList(STOP_WORDS);
			if(words.length>4){
				if(!((stopWords.contains(words[0]))||(stopWords.contains(words[1]))
						||(stopWords.contains(words[2]))||(stopWords.contains(words[3])||(stopWords.contains(words[4]))))){
					{
						if(counter%5==0){
						Text text = new Text();
						text.set(String.format("%s",strings[0]));
						Text text1 = new Text();
						text1.set(String.format("%s",strings[2]));
						context.write(text, text1);
						}
						counter++;
					}
				}
			}
		}
	}

		public static class Reduce extends Reducer<Text, Text, Text, Text> {
			@Override
			protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
				String w1 = key.toString();
				int sum_occ = 0;
				for (Text val : values) {
					sum_occ += Long.parseLong(val.toString());
				}
				Text newKey = new Text();
				newKey.set(String.format("%s",w1));
				Text newVal = new Text();
				newVal.set(String.format("%d",sum_occ));
				context.write(newKey, newVal);
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
			job.setJarByClass(create20precentCorpus.class);
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setPartitionerClass(create20precentCorpus.myPartitioner.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			//job.setInputFormatClass(TextInputFormat.class);
			//FileInputFormat.addInputPath(job, new Path(args[1]));
			job.setInputFormatClass(SequenceFileInputFormat.class);
			SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
			//String output="/smallCorpus/";
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			job.waitForCompletion(true);

		}

	}
