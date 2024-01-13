package org.kafkaApp.InitProducer.ProducerScript;

import org.kafkaApp.Configuration.CreateConfiguration;
import org.kafkaApp.Configuration.CreateTopic;
import org.kafkaApp.Serdes.Init.ListRequestStructure.ListRequestStructureSerializer;
import org.kafkaApp.Structure.RequestStructure;

import java.util.Properties;

public class ProducerQueryLoadReq {
    public ProducerQueryLoadReq() {

    }
    private void produceData(String topicName1, String topicName2, String[] dataPath,String RequestPath1)  {
        CreateConfiguration createConfiguration = new CreateConfiguration();

        Properties properties2 = createConfiguration.getPropertiesConfig(ListRequestStructureSerializer.class);

        // Build the Producer multithreaded
        CreateTopic createTopic = new CreateTopic();
        //createTopic.createMyTopic(topicName1, 3, 1);
        createTopic.createMyTopic(topicName2, 3, 1);

        Thread[] dispatchers = new Thread[1];

         dispatchers[0] = new Thread(new ProducerDispatcher(properties2, topicName2, RequestPath1, RequestStructure.class,0));
         dispatchers[0].start();
        try {
            for (Thread t : dispatchers)
                t.join();
        } catch (InterruptedException e) {
            System.err.println("Thread Interrupted ");
        }
    }
    public static void main(String[] args)  {

        String RequestPath1="C:/Query-loadReq.json";

        //the topic names
        String topicName1="Data_Topic";
        String topicName2="Request_Topic";


        //properties of the producer
        ProducerQueryLoadReq producer = new ProducerQueryLoadReq();
        producer.produceData(null,topicName2,null,RequestPath1);

    }
}
