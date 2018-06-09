import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/*Get the 1gram filters all the signs and unites identical words
 * */
public class step0 {
	public static final String[] STOP_WORDS = {"\"","׳","־","^","?",";",":","1",".",",","-","*","!",")","(","•","״","c","d","e","f","g","h","j","k","l","m","n","o","p","q","r","s","t","v","w","x","y","z","b","—","0","1","2","3","4","5","6","7","8","9","�","^","?",";",":","."
			,"|","£","¥","§","©","«","®","°","±","»","être","¿","à","B","C","D","E","F","■","O","<","₪","-","—","*","!","(",")","'","@","#","$","\"","%","&","[","]","{","}","-","/","+","=",".","'",",","~","�"};
	public static final String[] STOP_chars = {"\"","׳","־","^","?",";",":","1",".",",","-","*","!",")","(","•","״","�","^","?",";",":",".","—","0","1","2","3","4","5","6","7","8","9","�","^","?",";",":","."
			,"₪","-","—","*","!","(",")","'","@","#","$","\"","%","&","[","]","{","}","-","/","+","=",".","'",",","~","�"};
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {

			String[] strings = value.toString().split("\t");
			String w1 = strings[0];
			List<String> stopWords = Arrays.asList(STOP_WORDS);
			List<String> stopchar = Arrays.asList(STOP_chars);
			String[] s=w1.split("");
			boolean b=true;
			for(int i=0; i<s.length; i++){
				if(stopchar.contains(s[i])){
					b=false;
				}
			}
			if((!(stopWords.contains(w1))&&(b))){
				int occur = Integer.parseInt(strings[2]);
				Text text = new Text();
				text.set(String.format("%s",w1));
				Text text1 = new Text();
				Text text2=new Text();
				text2.set(String.format("*"));
				text1.set(String.format("%d",occur));
				context.write(text,text1);
				context.write(text2,text1);
			}
		}

	}

	private static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String w1 = key.toString();
			Long sum_occ = new Long(0);

			for (Text val : values) {
				sum_occ += Long.parseLong(val.toString());
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
		job.setJarByClass(step0.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step0.PartitionerClass.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
		/*to run local
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		 */
		String output="/output0/";
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);

	}
}





