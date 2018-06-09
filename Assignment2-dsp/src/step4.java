import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
public class step4 {
	/**
	 * The input:
	 *      Combines all the occurrences with the same key.
	 *               T n-gram   	T occurrences
	 *               program is good     	4                
	 * The Output:
	 *               T n-gram /T occurrences
	 *               program is	 program  is good	4
	 *           	 is good	 program  is good	4	   
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String[] words = strings[0].split(" ");
			String w1 = words[0];
			String w2 = words[1];   
			int occur = Integer.parseInt(strings[1]) ;
			Text text3=new Text();
			text3.set(String.format("%d",occur));
			Text text = new Text();
			text.set(String.format("%s %s",w1,w2));
			if(words.length>2){
				String w3=words[2];
				Text text1 = new Text();
				text1.set(String.format("%s %s %s %d",w1,w2,w3,occur));
				Text text2=new Text();
				text2.set(String.format("%s %s",w2,w3));
				context.write(text2, text1);
				context.write(text, text1);
			}
			else{
			context.write(text ,text3);
			}
		}
	}

	/**
	 * The input:
	 *      T n-gram /T occurrences
	 *      program is program is good 4
	 *      program is 	   5

	 * The Output:
	 *               T n-gram /T occurrences
	 *               program is good program  is	5   
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] strings = key.toString().split(" ");
			String w1 = strings[0];
			String w2 = strings[1];
			Text newKey = new Text();
			Text newVal = new Text();
			int occu=0;
			boolean b1=false;
			boolean b2=false;
			for (Text val : values) {
				String[] s=val.toString().split(" ");
				if(s.length>1){
					newKey.set(String.format("%s %s %s",s[0],s[1],s[2])); 
					b1=true;
				}
				else{
					occu=(int) Long.parseLong(s[0]);
					newVal.set(String.format("%s %s %d",w1,w2,occu));
					b2=true;
				}
				if(b1&&b2){
					context.write(newKey, newVal);
					b1=false;
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
			job.setJarByClass(step4.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setPartitionerClass(step4.myPartitioner.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			String input1="/output2/";
			String input2="/output3/";
			String output="/output4/";
			MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
			MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);
			FileOutputFormat.setOutputPath(job, new Path(output));
			job.waitForCompletion(true);
		}


}
