import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import java.io.IOException;
public class step3 {
	/**
     * The Input:
     *      Google 3gram database
     *              n-gram /T year /T occurrences /T pages /T books
     *              program is good    1991    3   2   1
     *
     * The Output:
     *      For each line from the input it creates a line with the word and its occurrences.
     *               T n-gram /T occurrences
     *               program  is good		3  
     *               program  is good       1  
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] strings = value.toString().split("\t");
            String[] words = strings[0].split(" ");
            if(words.length>2){
            String w1 = words[0];
            String w2 = words[1];
            String w3= words[2];
            int occur = Integer.parseInt(strings[2]);
            Text text = new Text();
            text.set(String.format("%s %s %s",w1,w2,w3));
            Text text1 = new Text();
            text1.set(String.format("%d",occur));
            context.write(text ,text1);
            }
        }

    }

    
    /**
     * Input:
     *      The input is the sorted output of the mapper 
     *      Maybe output from different mappers.
     *      Template:
     *               T n-gram /T occurrences
     *                 program  is good		3  
     *                 program  is good     1 
     *
     * Output:
     *      Combines all the occurrences with the same key.
     *               T n-gram   	T occurrences
     *               program is good     	4   
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

    	@Override
    	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		String oldKey=key.toString();
    		int sum_occ = 0;
    		for (Text val : values) {
    			sum_occ += Long.parseLong(val.toString());
    		}
    		Text newKey = new Text();
    		newKey.set(String.format("%s",oldKey));
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
		job.setJarByClass(step3.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step3.myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		String output="/output3/";
		SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
		
	}


}
