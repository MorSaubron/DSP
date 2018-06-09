import org.apache.hadoop.mapreduce.TestMapCollection.StepFactory;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Main {
	public static AWSCredentialsProvider credentialsProvider;
	public static AmazonS3 S3;
	public static AmazonEC2 ec2;
	public static AmazonElasticMapReduce emr;
	public static void main(String[]args){
		
		credentialsProvider =new EnvironmentVariableCredentialsProvider();			

		System.out.println("===========================================");
		System.out.println("connect to aws & S3");
		System.out.println("===========================================\n");

		S3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();
		//delete the output file if it exist
				ObjectListing objects = S3.listObjects("assignment2dspmor", "outputAssignment2");
				for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
					S3.deleteObject("assignment2dspmor", s3ObjectSummary.getKey());
				}
		System.out.println("===========================================");
		System.out.println("connect to aws & ec2");
		System.out.println("===========================================\n");

		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();
		System.out.println("creating a emr");
		 emr= AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-west-2")
				.build();
		 
		System.out.println( emr.listClusters());
		 
		StepFactory stepFactory = new StepFactory();
		/*
        step1
		 */
		HadoopJarStepConfig step1 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step1.jar")
				.withArgs("step1","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data");

		StepConfig stepOne = new StepConfig()
				.withName("step1")
				.withHadoopJarStep(step1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step2
		 */
		HadoopJarStepConfig step2 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step2.jar")
				.withArgs("step2","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data");

		StepConfig stepTwo = new StepConfig()
				.withName("step2")
				.withHadoopJarStep(step2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step3
		 */
		HadoopJarStepConfig step3 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step3.jar")
				.withArgs("step3","null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data");

		StepConfig stepThree = new StepConfig()
				.withName("step3")
				.withHadoopJarStep(step3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step4
		 */
		HadoopJarStepConfig step4 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step4.jar")
				.withArgs("step4");

		StepConfig stepFour = new StepConfig()
				.withName("step4")
				.withHadoopJarStep(step4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step5
		 */
		HadoopJarStepConfig step5 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step5.jar")
				.withArgs("step5");

		StepConfig stepFive = new StepConfig()
				.withName("step5")
				.withHadoopJarStep(step5)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step6
		 */
		HadoopJarStepConfig step6 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step6.jar")
				.withArgs("step6","null","s3n://assignment2dspmor//outputAssignment2");

		StepConfig stepSix = new StepConfig()
				.withName("step6")
				.withHadoopJarStep(step6)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(3)
				.withMasterInstanceType(InstanceType.M3Xlarge.toString())
				.withSlaveInstanceType(InstanceType.M3Xlarge.toString())
				.withHadoopVersion("2.7.3")                                 
				.withEc2KeyName("morKP")         
				.withPlacement(new PlacementType("us-west-2a"))
				.withKeepJobFlowAliveWhenNoSteps(false);

		System.out.println("give the cluster all our steps");
		RunJobFlowRequest request = new RunJobFlowRequest()
				.withName("Assignment2")                                   
				.withInstances(instances)
				.withSteps(stepOne,stepTwo,stepThree,stepFour,stepFive,stepSix)
				.withLogUri("s3n://assignment2dspmor/logs/")
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel("emr-5.11.0");

		RunJobFlowResult result = emr.runJobFlow(request);
		String id=result.getJobFlowId();
		System.out.println("our cluster id: "+id);

	}
}

