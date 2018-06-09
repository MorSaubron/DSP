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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import java.io.IOException;
import java.util.HashMap;


public class step5 {
	/**
	 * The input:
	 *      Combines all the occurrences with the same key.
	 *             T n-gram /T occurrences
	 *            program is good program  is	5  
	 *            program is good is good		4            
	 * The Output:
	 *               T n-gram /T occurrences
	 *               program is good program  is	5  
	 *               program is good is good		4   
	 */
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map (LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] strings = value.toString().split("\t");
			String[] words1 = strings[0].split(" ");
			String w1 = words1[0];
			String w2 = words1[1]; 
			String w3= words1[2];
			String[]words2=strings[1].split(" ");
			int occur =0;
			Text text = new Text();
			text.set(String.format("%s %s %s",w1,w2,w3));
			if(words2.length>1){
				occur= Integer.parseInt(words2[2]) ;
				Text text2=new Text();
				text2.set(String.format("%s %s %d",words2[0],words2[1],occur));
				context.write(text, text2);
			}
			else{
				occur= Integer.parseInt(strings[1]) ;
				Text text1 = new Text();
				text1.set(String.format("%d",occur));
				context.write(text ,text1);
			}		
		}
	}
	
	
	/**
	 * The input:
	 *      T n-gram /T occurrences
	 *      program is good program is	5  
	 *      program is good is good		4
	 *    	program is good 			4  
	 *		OR the output of step 1.    
	 * The Output:
	 *               T n-gram /T prob
	 *               program is good   0.6
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public static Long c0=new  Long(0);
		public static HashMap <String, Double> map= new HashMap<String,Double>(); 
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			String[] strings = key.toString().split(" ");
			String w1 = strings[0];
			String w2 = strings[1];
			String w3= strings[2];
			Double N3=0.0;
			Double N2=0.0;
			Double N1=0.0;
			Double C1=0.0;
			Double k2=0.0;
			Double k3=0.0;
			Double C2=0.0;
			Double prob=0.0;
			Text newKey = new Text();
			Text newVal = new Text();
			N1=map.get(w3);
			C1=map.get(w2);
			boolean b1=false;
			boolean b2=false;
			for (Text val : values) {
				String[] s=val.toString().split(" ");
				if(s.length<2){
					N3=(double) Long.parseLong(s[0]);
					k3=(Math.log(N3+1)+1)/(Math.log(N3+1)+2);
					
				}
				else{
					if(s[0].equals(w1)&&s[1].equals(w2)){	
						C2=(double) Long.parseLong(s[2]);
						b1=true;
					}
					else{
						if(s[0].equals(w2)&&s[1].equals(w3))
							N2=(double) Long.parseLong(s[2]);
						k2=(Math.log(N2+1)+1)/(Math.log(N2+1)+2);
						b2=true;
					}
				}
				if(C1!=null&&N1!=null&&b1&&b2){
					prob=(k3*(N3/C2))+((1-k3)*k2*(N2/C1))+((1-k3)*(1-k2)*(N1/c0));
					newKey.set(String.format("%s %s %s",w1,w2,w3));
					newVal.set(String.format("%s",prob));
					context.write(newKey, newVal);
				}

			}

		}

		public void setup(Reducer.Context context) throws IOException {  
			FileSystem fileSystem = FileSystem.get(context.getConfiguration());
			RemoteIterator<LocatedFileStatus> it=fileSystem.listFiles(new Path("/output1"),false);
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
						else{
							map.put(ones[0], (double) Long.parseLong(ones[1]));
						}
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
		job.setJarByClass(step5.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(step5.myPartitioner.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		String input1="/output4/";
		String input2="/output3/";
		String output="/output5/";
		MultipleInputs.addInputPath(job, new Path(input1), TextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class);	
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}

}
