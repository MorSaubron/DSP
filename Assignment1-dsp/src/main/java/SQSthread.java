
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import java.util.concurrent.atomic.AtomicInteger;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;


import javax.jms.*;

public class SQSthread implements Runnable {
    public static AtomicInteger count = new AtomicInteger(0);
    public static Session session;
    public static SQSConnection connection;

    @Override
    public void run() {
        Manager.credentialsProvider = new AWSStaticCredentialsProvider
                (new InstanceProfileCredentialsProvider(false).getCredentials());
//        Manager.credentialsProvider = new AWSStaticCredentialsProvider
//                (new ProfileCredentialsProvider().getCredentials());      //to run local

        Manager.connectionFactory = new SQSConnectionFactory(
                new ProviderConfiguration(),
                AmazonSQSClientBuilder.standard()
                        .withRegion("us-west-2")
                        .withCredentials(Manager.credentialsProvider)
        );
        try {

            connection = Manager.connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer consumer = session.createConsumer(session.createQueue("AppToManager"));
            connection.start();
            Message message;
               while (true) {
                   while ((message = consumer.receive()) == null) {
                       System.out.println("There is no new msg in the app-manager thread");
                       Thread.sleep(1000);
                       if (Thread.interrupted()) {
                           return;
                       }
                   }
                   Manager.queue.add(message);
               }
        } catch (JMSException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println("The sqs thread is finish");
            return;
        }finally {
            try {
                session.close();
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }

        }
    }
}





