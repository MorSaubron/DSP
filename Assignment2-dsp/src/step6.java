import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class step6 {
	
	 /**
     * The Input:
     *      The input is the output of Step 5
     *         T  n-gram  /T  prob
     *         program is good   0.4
     *
     * The Output:
     *      Same data,ordered by the key.
     *        T  n-gram  /T  prob
     *        program is good  0.4
     *
     */
    private static class Map extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            Text key1 = new Text();
            key1.set(String.format("%s %s",splits[0],splits[1]));
            Text newValue = new Text();
          	newValue.set(String.format("%s",""));
            context.write(key1,newValue);
        }

    }
    
    /**
     * The Input:
     *      The output of the map after ordered by the value(the prob)
     *         T  n-gram  /T  prob
     *         program is good   0.4
     *         program is bad    0.3
     *
     * The Output:
     *      Same data.
     *
     */

    private static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String w1 = key.toString();
			Text newKey = new Text();
			newKey.set(String.format("%s",w1));
			Text newVal = new Text();
			newVal.set(String.format("%s",""));
			context.write(newKey, newVal);
		}
			
	}

	private static class CompareClass extends WritableComparator {
	        protected CompareClass() {
	            super(Text.class, true);
	        }
	        @Override
	        public int compare(WritableComparable key1, WritableComparable key2) {
	            String[] splits1 = key1.toString().split(" ");
	            String[] splits2 = key2.toString().split(" ");
	            if (splits1[0].equals(splits2[0])&& splits1[1].equals(splits2[1])) {
	                if(Double.parseDouble(splits1[3])>(Double.parseDouble(splits2[3]))){
	                        return -1;
	                    }
	                    else
	                        return 1;
	                }
	            return (splits1[0]+" "+splits1[1]).compareTo(splits2[0]+" "+splits2[1]);

	            }
	        }


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
		job.setJarByClass(step6.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setSortComparatorClass(step6.CompareClass.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(1);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        String input="/output5/";
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
