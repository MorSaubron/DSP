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
				ObjectListing objects = S3.listObjects("morelad", "outputAssignment3");
				for (S3ObjectSummary s3ObjectSummary : objects.getObjectSummaries()) {
					S3.deleteObject("morelad", s3ObjectSummary.getKey());
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
        step0
		 */
		HadoopJarStepConfig step0 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step0.jar")
				.withArgs("null","s3n://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/1gram/data");

		StepConfig stepZero = new StepConfig()
				.withName("step0")
				.withHadoopJarStep(step0)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		
		/*
        stepCreateHfw
		 */
		HadoopJarStepConfig stepCreateHFWConfig = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/stepCreateHFW.jar");

		StepConfig stepCreateHFW = new StepConfig()
				.withName("stepCreateHFW")
				.withHadoopJarStep(stepCreateHFWConfig)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		/*
        stepCreateHooksTarget
		 */
		HadoopJarStepConfig stepCreateHooksTargetConfig = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/stepCreateHooksTarget.jar");
		
		StepConfig stepCreateHooksTarget = new StepConfig()
				.withName("stepCreateHooksTarget")
				.withHadoopJarStep(stepCreateHooksTargetConfig)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		
		/*
        step1
		 */
		HadoopJarStepConfig step1 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step1.jar")
				.withArgs("null","s3://morelad/smallCorpus/","s3://morelad/outputstep1");

		StepConfig stepOne = new StepConfig()
				.withName("step1")
				.withHadoopJarStep(step1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step2
		 */
		HadoopJarStepConfig step2 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step2.jar")
				.withArgs("null","s3://morelad/outputstep1/","s3://morelad/output2/");

		StepConfig stepTwo = new StepConfig()
				.withName("step2")
				.withHadoopJarStep(step2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
		/*
        step3
		 */
		HadoopJarStepConfig step3 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step3.jar")
				.withArgs("null","s3://morelad/output2/","s3://morelad/outputPatt/");

		StepConfig stepThree = new StepConfig()
				.withName("step3")
				.withHadoopJarStep(step3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");
				
		/*
        step4
		 */
		HadoopJarStepConfig step4 = new HadoopJarStepConfig()
				.withJar("s3://assignment2dspmor/step4.jar")
				.withArgs("null","s3://morelad/output2/","s3://morelad/outputSameHooks/");

		StepConfig stepfour = new StepConfig()
				.withName("step4")
				.withHadoopJarStep(step4)
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
				.withName("Assignment3")                                   
				.withInstances(instances)
				.withSteps(stepZero,stepCreateHFW,stepCreateHooksTarget,stepOne,stepTwo,stepThree,stepfour)
				.withLogUri("s3n://morelad/logs/")
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel("emr-5.11.0");

		RunJobFlowResult result = emr.runJobFlow(request);
		String id=result.getJobFlowId();
		System.out.println("our cluster id: "+id);

	}
}

