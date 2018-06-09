import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.gamelift.model.DescribeInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.google.gson.Gson;
import javax.jms.*;
import java.io.*;
import java.util.*;

public class LocalApp {
    public static AmazonS3 S3;
    public static AmazonEC2 ec2;
    public static Map<String, String> arguments;
    public static SQSConnectionFactory connectionFactory;
    public static RunInstancesRequest request;
    public static List<Instance> instances;
    public static String listOfin;
    public static String bucketName;
    public static AWSCredentialsProvider credentialsProvider;
    public static Vector<String> keys = new Vector<String>();
    public static boolean terminate = false;
    public static IamInstanceProfileSpecification instanceP;
    private static UUID uuid;
    private static int workersNum;
    private static int numberOfFiles;
    public static  Connection connection = null;
    public static Session session;

    public static void main(String[] args) throws Exception {
        init(args);
        startS3("C:\\Users\\Mor\\IdeaProjects\\Assignment1");
        UpToS3("../docs");
        createQueue();
        if (terminate)
            Terminate();
        sendWorkersNumMessage();
        sendMesage();
        while (numberOfFiles > 0) {
            getOutput();
        }
        if(terminate) {
            List<String> l = new ArrayList<String>();
            for (int i = 0; i < instances.size(); i++) {
                l.add(instances.get(i).getInstanceId());
            }
            TerminateInstancesRequest terminateRequest = new TerminateInstancesRequest(l);
            ec2.terminateInstances(terminateRequest);
            closeAll();
        }
        deleteTheQueue();
        session.close();
        connection.close();
        ec2.shutdown();
        S3.shutdown();
        System.out.println("the local app is finished "+numberOfFiles);
        System.exit(0);
    }

    private static void closeAll() {
        connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                        .withRegion("us-west-2")
                        .withCredentials(credentialsProvider)
        );
        try {
            SQSConnection connection = connectionFactory.createConnection();
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
            client.getAmazonSQSClient().deleteQueue("AppToManager");
            connection.close();

        } catch (JMSException e) {
            System.out.println("all the queue deleted");
        }
    }

    //initilize and creating instance
    public static void init(String[] args) {
        createMap(args);
        uuid = UUID.randomUUID();
        credentialsProvider = new AWSStaticCredentialsProvider(
                new EnvironmentVariableCredentialsProvider().getCredentials());
        ec2 = AmazonEC2ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        Script managerBash = new Script();
        managerBash.setManagerScript();
        instanceP=new IamInstanceProfileSpecification();
        instanceP.setArn("arn:aws:iam::504703692217:instance-profile/ManagerRole");
        if (!isActive()) {
            try {
                request = new RunInstancesRequest("ami-32d8124a", 1, 1);
                request.setInstanceType(InstanceType.T2Micro.toString());
                request.withKeyName("morKP");
                request.withSecurityGroups("mor");
                request.withUserData(managerBash.getManagerScript());
                request.setIamInstanceProfile(instanceP);
                instances = ec2.runInstances(request).getReservation().getInstances();
                System.out.println("create instances: " + instances);

            } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
            }
        }
    }

    //check if the manager is active- return true/false
    public static boolean isActive() {
        DescribeInstancesRequest dis = new DescribeInstancesRequest();
        listOfin = dis.getInstanceId();
        DescribeInstancesResult disresult = ec2.describeInstances();
        List<Reservation> list = disresult.getReservations();
        System.out.println("-------------- status of instances -------------");
        if (list.size() > 0) {
            for (Reservation res : list) {
                List<Instance> instancelist = res.getInstances();
                for (Instance instance : instancelist) {
                    if (instance.getState().getName().equals("running")) {
                        System.out.println("The Manager is allready active");
                        return true;
                    }
                    List<Tag> t1 = instance.getTags();
                    for (Tag teg : t1) {
                        System.out.println("Instance Name   : " + teg.getValue());
                    }
                }
            }
        }
        return false;
    }
    //create a input-output map
    public static void createMap(String[] args) {
        arguments = new HashMap<String, String>();
        int length = 0;
        if (args[args.length - 1].toLowerCase().equals("terminate")) {
            terminate = true;
            // terminate arg was supplied - second from end arg is # of workers
            workersNum = Integer.parseInt(args[args.length - 2]);
            length = (args.length - 2) / 2;
        } else { //no terminate arg, last arg is # of workers
            workersNum = Integer.parseInt(args[args.length - 1]);
            length = (args.length - 1) / 2;
        }

        for (int i = 0; i < length; i++) {
            arguments.put(args[i], args[length + i]);
        }
        numberOfFiles = arguments.size();
    }

    //send the num of workers
    private static void sendWorkersNumMessage() {
        WorkersNumMsg workersNumMsg = new WorkersNumMsg(workersNum,uuid);
        Gson gson = new Gson();
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue("AppToManager"));
            TextMessage message = session.createTextMessage(gson.toJson(workersNumMsg).toString());
            producer.send(message);
            producer.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    //create a connection to the S3
    public static void startS3(String directoryName) {
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-west-2")
                .build();
        bucketName = credentialsProvider.getCredentials()
                .getAWSAccessKeyId() + "-" + directoryName.replace
                ('\\', '-').replace('/', '-').replace
                (':', '-');
        bucketName = bucketName.toLowerCase();
        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");
        try {
            System.out.println("Creating bucket " + bucketName + "\n");
            S3.createBucket(bucketName);
            /*
             * List the buckets in your account
             */
            System.out.println("Listing buckets");
            for (Bucket bucket : S3.listBuckets()) {
                System.out.println(" - " + bucket.getName());
            }
            System.out.println();
        } catch (AmazonServiceException ace) {
            System.out.println("Start s3");
            System.out.println("Caught an AmazonClientException, " +
                    "which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    //upload the files to S3
    public static void UpToS3(String directoryName) {
        try {
            System.out.println("Uploading a new object to S3 from a file");
            int i = 0;
            File dir = new File(directoryName);
            for (File file : dir.listFiles()) {
                if (arguments.containsKey(file.getName())) {
                    keys.add(file.getName().replace
                            ('\\', '-').replace('/', '-').
                            replace(':', '-'));
                    PutObjectRequest req = new PutObjectRequest(bucketName, keys.elementAt(i), file).withCannedAcl(CannedAccessControlList.PublicRead);
                    S3.putObject(req);
                    i++;
                }
            }
        } catch (AmazonServiceException ace) {
            System.out.println("Uploading");
            System.out.println("Caught an AmazonClientException," +
                    " which means the client encountered "
                    + "a serious internal problem while trying to communicate with " + "S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    //create the queues for manager- localApp communication
    public static void createQueue() {
        System.out.println("start a connection with the sqs");
        connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                        .withRegion("us-west-2")
                        .withCredentials(credentialsProvider)
        );
        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon SQS");
        System.out.println("===========================================\n");
        try {
            SQSConnection connection = connectionFactory.createConnection();
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
            System.out.println("Creating a new SQS queue called ManagerToApp.\n");
            if (!client.queueExists("ManagerToApp" + uuid.toString().toLowerCase())) {
                client.createQueue("ManagerToApp" + uuid.toString().toLowerCase());
            }
            System.out.println("Creating a new SQS queue called AppToManager.\n");
            if (!client.queueExists(("AppToManager"))) {
                client.createQueue("AppToManager");
            }
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    //send a URLmsg
    public static void sendMesage() {
        SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                        .withRegion("us-west-2")
                        .withCredentials(credentialsProvider)
        );

        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue("AppToManager"));
            for (int i = 0; i < keys.size(); i++) {
                System.out.println("Sending a " + i + "message to ApptoMamager\n");
                System.out.println("the key:" + keys.elementAt(i).toString());
                UrlMsg urlMsg = new UrlMsg(bucketName, keys.elementAt(i), S3.getUrl(bucketName, keys.elementAt(i)).toString(), uuid);
                Gson gson = new Gson();
                TextMessage message = session.createTextMessage(gson.toJson(urlMsg));
                producer.send(message);
            }
            producer.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    //get an output from the queue
    public static void getOutput() {
        Message message = null;
        try {
            SQSConnection connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
            MessageConsumer consumer = session.createConsumer(session.createQueue("ManagerToApp" + uuid.toString()));
            connection.start();
            message = consumer.receive();
            OutputMsg outputMsg = new Gson().fromJson(((TextMessage) message).getText(), OutputMsg.class);
            createHTML(outputMsg);
            connection.close();
//            session.close();
            consumer.close();
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void createHTML(OutputMsg outputMsg) throws IOException {
        File file = getFromS3(outputMsg.getFileName());
        FileReader fileReader = new FileReader(file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        StringBuffer stringBuffer = new StringBuffer();
        String line;
        String outputName = outputMsg.getFileName();
        BufferedWriter bw = new BufferedWriter(new FileWriter(arguments.get(outputName.substring(0,outputName.length()-1))));
        bw.write("<html>");
        bw.write("<body>");
        Gson gson = new Gson();
        while ((line = bufferedReader.readLine()) != null) {
            ReviewResponse reviewResponse = gson.fromJson(line, ReviewResponse.class);
            bw.write(makeHTMLLine(reviewResponse));
        }
        bw.write("</body>");
        bw.write("</html>");
        bw.close();
        numberOfFiles--;
        System.out.println("the num of files left: "+numberOfFiles);
    }

    public static String makeHTMLLine(ReviewResponse reviewResponse){
        String color = "";
        switch (reviewResponse.getSentiment()) {
            case 0:
                color = "DarkRed";
                break;
            case 1:
                color = "red";
                break;
            case 2:
                color = "black";
                break;
            case 3:
                color = "LightGreen";
                break;
            case 4:
                color = "DarkGreen";
                break;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("<p>");
        builder.append("Review: ");
        builder.append("<span style=\"color: "+color+";\">");
        builder.append(reviewResponse.getReview().getText());
        builder.append("</span>");
        builder.append("<a href=\""+reviewResponse.getReview().getLink()+"\">, " + reviewResponse.getReview().getLink() + "</a>");
        builder.append(", Named entities: " + reviewResponse.getM());
        if (reviewResponse.isSarcasm()){
            builder.append(", <b>This review is sarcastic</b>");
        }else {
            builder.append(", <b>This review is NOT sarcastic</b>");
        }
        builder.append("<hr size=\"10\">");
        builder.append("<br></p>\n");
        return builder.toString();
    }

    private static File getFromS3(String s) {
        File file = new File(s);
        S3Object obj = S3.getObject(bucketName, s);
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
        return file;
    }


    public static void Terminate() {
        TerminateMsg terminateMsg = new TerminateMsg(uuid);
        Gson gson = new Gson();
        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue("AppToManager"));
            TextMessage message = session.createTextMessage(gson.toJson(terminateMsg));
            producer.send(message);
            producer.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }

    }

    private static void deleteTheQueue() {
        connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                        .withRegion("us-west-2")
                        .withCredentials(credentialsProvider)
        );
        try {
            SQSConnection connection = connectionFactory.createConnection();
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();
            client.getAmazonSQSClient().deleteQueue("ManagerToApp"+uuid);
            connection.close();

        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}


