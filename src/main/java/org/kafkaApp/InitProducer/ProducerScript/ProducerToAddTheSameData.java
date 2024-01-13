package org.kafkaApp.InitProducer.ProducerScript;

import org.kafkaApp.Configuration.CreateConfiguration;
import org.kafkaApp.Serdes.Init.ListDataStructure.ListDataStructureSerializer;
import org.kafkaApp.Structure.DataStructure;

import java.util.Properties;

public class ProducerToAddTheSameData {
    public ProducerToAddTheSameData() {

    }
    private void produceData(String topicName1, String[] dataPath) {
        CreateConfiguration createConfiguration = new CreateConfiguration();

        Properties properties = createConfiguration.getPropertiesConfig(ListDataStructureSerializer.class);


        Thread[] dispatchers = new Thread[3];
        for (int i = 0; i < 3; i++) {

            dispatchers[i] = new Thread(new ProducerDispatcher(properties, topicName1, dataPath[i],DataStructure.class,i));
            dispatchers[i].start();
        }

        try {
            for (Thread t : dispatchers)
                t.join();
        } catch (InterruptedException e) {
            System.err.println("Thread Interrupted ");
        }
    }
    public static void main(String[] args)  {

        //the data path of the data, wanted to send to the kafka

        String dataPath1="C:\\EURTRY_small_addNew.json";
        String dataPath2="C:\\FutureND_small_addNew.json";
        String dataPath3="C:\\XAUUSD_small_addNew.json";

        String[] dataPath={dataPath1,dataPath2,dataPath3};
        //the request path of the data, wanted to send to the kafka


        //the topic names
        String topicName1="Data_Topic";


        //properties of the producer
        ProducerToAddTheSameData producer = new ProducerToAddTheSameData();
        producer.produceData(topicName1,dataPath);

    }
}
