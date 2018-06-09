import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.util.Pair;
import org.apache.http.entity.FileEntity;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
public class Algorithm {
	public static AllCorpuses all=new AllCorpuses();
	public static AmazonS3 S3;
	public static AmazonEC2 ec2;
	public static AWSCredentialsProvider credentialsProvider;
	public static S3Object obj = new S3Object();
	public static HashSet<String> Patterns = new HashSet<>();
	private static String bucketName = "morelad";
	private static File folder = new File("output2");
	private static File folderPattern = new File("outputPatt");
	private static File folderHooks = new File("outputSameHooks");
	//public static HashMap<String, ArrayList<String>> hooksSamePatt=new HashMap<>();

	public static void main(String[] args) throws Exception {
		//create a connection to s3
		initialize();
		//download the output of step 2
		getFromS3("output2");
		//download the output of step 3- all the patterns with more than one hook
		createPattList();
		//create the all- which contains all the hook corpses
		readFiles();
		//download the output of step 4- all the hooks from the same patterns- this is an improvement
		//createSameHooks();
		//make the steps of the algorithm
		doTheAlg();
		//create the hits vector- write them to a file
		createTheHitsVector();
	}


	private static void createTheHitsVector() {
		System.out.println("create the vectors");
		try {
			BufferedReader br = new BufferedReader(new FileReader("dataset.txt"));
			String line;
			String[] words;
			String hit = "";
			line = br.readLine();
			while(null != line){
				words = line.split("\t");
				for (HookCorpus h1 : all.getH().values()) {
					for(Clusters c1:h1.getClusters()){
						hit = hit +","+ c1.calcTheHits(words[0], words[1]);
					}
				}
				words[2]=words[0]+" "+words[1]+" "+words[2];
				writeTheVectores(hit , words[2]);
				hit="";
				line = br.readLine();
			}



		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void writeTheVectores(String hit, String word) {
		try(FileWriter fw = new FileWriter("vectors22.txt", true);
				BufferedWriter bw = new BufferedWriter(fw);
				PrintWriter out = new PrintWriter(bw))
		{
			out.println(word+hit);
		} catch (IOException e) {
			//TODO Auto-generated catch block
		}
	}

//	private static void createSameHooks() {
//		getFromS3("outputSameHooks");
//		for (final File fileEntry :folderHooks.listFiles()) {
//			if (fileEntry.getName().startsWith("part")){
//				BufferedReader br = null;
//				try {
//					br = new BufferedReader(new FileReader(fileEntry));
//					String line;
//					String[] ones;
//					while ((line = br.readLine()) != null) {
//						ones=line.split(";");
//						ArrayList<String>l=new ArrayList<String>();
//						for(int k=1;k<ones.length;k++){
//							l.add(ones[k]);
//						}
//						hooksSamePatt.put(ones[0],l);
//					}
//				} catch (IOException e) {
//
//					e.printStackTrace();
//
//				} finally {
//
//					try {
//
//						if (br != null)
//							br.close();
//
//					} catch (IOException ex) {
//
//						ex.printStackTrace();
//
//					}
//
//				}
//			}
//		}
//	}

	private static void doTheAlg() {
		ArrayList<Clusters> delClus= new ArrayList<Clusters>();
		for (HookCorpus h1 : all.getH().values()) {
			while(h1.CehckIfClustersEx()){
				Clusters c1=h1.minimalClus();
				for (HookCorpus h2 : all.getH().values()) {
					if(!h2.equalsTo(h1) && !h2.getVisited()){
						//if((hooksSamePatt.get(h1.getHook()))!=null&&(hooksSamePatt.get(h1.getHook()).contains(h2.getHook()))){
							for(Clusters c2:h2.getClusters()){
								if(h1.canWeMerged(c2,c1)){
									delClus.add(c2);
								}			
							}
							h2.DeleteClusters(delClus);
							delClus.clear();
						}
					//}
				}
				if(c1.unconfirmedPatternsOnly()){
					h1.DeleteCluster(c1);
				}
			}
			h1.setVisited();
		}
		System.out.println("done with the alg");
	}

	private static void createPattList() {
		getFromS3("outputPatt");
		for (final File fileEntry :folderPattern.listFiles()) {
			if (fileEntry.getName().startsWith("part")){
				BufferedReader br = null;
				try {
					br = new BufferedReader(new FileReader(fileEntry));
					String line;
					String[] ones;
					while ((line = br.readLine()) != null) {
						ones=line.split("\t");
						Patterns.add(ones[0]);
					}
				} catch (IOException e) {

					e.printStackTrace();

				} finally {

					try {

						if (br != null)
							br.close();

					} catch (IOException ex) {

						ex.printStackTrace();

					}

				}

			}
		}
		System.out.println("get the patterns and create a list");
	}

	public static void getFromS3(String folder) {
		ArrayList<String> keys = geyKeysFromS3(folder);
		for(String key:keys){
			File file = new File(key);
			obj = S3.getObject(bucketName,key);
			InputStream reader = new BufferedInputStream(
					obj.getObjectContent());
			OutputStream writer = null;
			try {
				writer = new BufferedOutputStream(new FileOutputStream(file));
				int read = -1;
				while ((read = reader.read()) != -1) {
					writer.write(read);
				}
				writer.flush();
				writer.close();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("get the files from S3");
	}

	private static ArrayList<String> geyKeysFromS3(String folder) {
		ListObjectsRequest listObjectsRequest = 
				new ListObjectsRequest()
				.withBucketName(bucketName)
				.withPrefix(folder + "/");

		ArrayList<String> keys = new ArrayList<>();
		ObjectListing objects = S3.listObjects(listObjectsRequest);
		for (;;) {
			List<S3ObjectSummary> summaries = objects.getObjectSummaries();
			if (summaries.size() < 1) {
				break;
			}
			summaries.forEach(s -> keys.add(s.getKey()));
			objects = S3.listNextBatchOfObjects(objects);
		}

		return keys;
	}

	public static void initialize() {
		//		credentialsProvider = new AWSStaticCredentialsProvider 
		//				(new InstanceProfileCredentialsProvider(false).getCredentials());

		credentialsProvider = new AWSStaticCredentialsProvider
				(new ProfileCredentialsProvider().getCredentials());          //to ran local

		System.out.println("===========================================");
		System.out.println("connect to aws & ec2");
		System.out.println("===========================================\n");
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();

		System.out.println("========================");
		System.out.println("connect to S3");
		S3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();

	}


	public static void readFiles(){
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.getName().startsWith("part")){
				// this file here is partxxxxx
				BufferedReader br = null;
				try {
					br = new BufferedReader(new FileReader(fileEntry));
					String line;
					String[] ones;
					while ((line = br.readLine()) != null) {
						ones=line.split("\t");
						HookCorpus h= new HookCorpus(ones[0]);
						for(int i=1; i<ones.length;i++){
							String [] patt=ones[i].split(";");
							Clusters c=new Clusters(patt[patt.length-1]);
							for(int k=0;k<patt.length-1;k++){
								if(Patterns.contains(patt[k])){
									Pattern p= new Pattern(patt[k], false);
									p.addHookTarget(ones[0], patt[patt.length-1]);
									c.AddUnconfimedPattern(p);
								}
							}
							if (c.getUnconfirmedPatterns().size() != 0){
								h.AddCluster(c);
							}
						}
						all.AddCorpuses(h.getHook(), h);
					}

				} catch (IOException e) {

					e.printStackTrace();

				} finally {

					try {

						if (br != null)
							br.close();

					} catch (IOException ex) {

						ex.printStackTrace();

					}

				}
			}
		}
		System.out.println("create all");
	}
}


