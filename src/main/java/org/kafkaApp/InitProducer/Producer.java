package org.kafkaApp.InitProducer;


import org.kafkaApp.Configuration.CreateConfiguration;
import org.kafkaApp.Configuration.EnvironmentConfiguration;
import org.kafkaApp.InitProducer.ProducerScript.ProducerDispatcher;
import org.kafkaApp.InitProducer.ProducerScript.RealTimeProduceData;
import org.kafkaApp.Serdes.Init.DataStructure.DataStructureSerializer;
import org.kafkaApp.Serdes.Init.ListRequestStructure.ListRequestStructureSerializer;
import org.kafkaApp.Structure.RequestStructure;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Producer {
    private final  static int replicateFactor = 3;
    public Producer() {

    }
    public void produceRequest(String topicname,String datapath){
        CreateConfiguration createConfiguration = new CreateConfiguration();
        Properties properties2 = createConfiguration.getPropertiesConfig(ListRequestStructureSerializer.class);
        Thread[] dispatchers = new Thread[1];
        dispatchers[0] = new Thread(new ProducerDispatcher(properties2, topicname, datapath,RequestStructure.class,0));
        dispatchers[0].start();
        try {
            for (Thread t : dispatchers)
                t.join();
        } catch (InterruptedException e) {
            System.err.println("Thread Interrupted ");
        }
    }
    private void produceData(String topicName1, String[] dataPath)  {
        CreateConfiguration createConfiguration = new CreateConfiguration();

        Properties properties = createConfiguration.getPropertiesConfig(DataStructureSerializer.class);

        Thread[] dispatchers = new Thread[dataPath.length];
        for (int i = 0; i < dataPath.length; i++) {
            int partition = i % EnvironmentConfiguration.giveTheParallelDegree();
            System.out.println("PARTITION I AM"+partition);
            dispatchers[i] = new Thread(new RealTimeProduceData( topicName1,dataPath[i],properties,partition));
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
       /* int totalStocks=9;
        //stpcks Name for Forex
        String [] forexStocksNames = new String[totalStocks];
        forexStocksNames[0] = "Forex·EURTRY·NoExpiry.json";
        forexStocksNames[1] = "Forex·XAUUSD·NoExpiry.json";
        forexStocksNames[2] = "Forex·AUDCHF·NoExpiry.json";
        forexStocksNames[3] = "Forex·AUDCAD·NoExpiry.json";
       /* forexStocksNames[4] = "Forex·AUDNZD·NoExpiry.json";
        forexStocksNames[5] = "Forex·AUDJPY·NoExpiry.json";

       /* forexStocksNames[6] = "Forex·AUDUSD·NoExpiry.json";
        forexStocksNames[7] = "Forex·CADCHF·NoExpiry.json";
        forexStocksNames[8] = "Forex·CADJPY·NoExpiry.json";
        /*forexStocksNames[9] = "Forex·CHFJPY·NoExpiry.json";
        forexStocksNames[10] = "Forex·EURAUD·NoExpiry.json";
        forexStocksNames[11] = "Forex·EURCAD·NoExpiry.json";*/
        //the data path of the data, wanted to send to the kafka
       /* String subDataPath="C:\\dataset\\ForexStocks\\";
        String[] TotalDataPath=new String[totalStocks];

        for (int i = 0; i < forexStocksNames.length; i++) {
            TotalDataPath[i]=subDataPath + forexStocksNames[i];
        }*/


        File folder = new File(EnvironmentConfiguration.getFilePathForDataTopic());
        FilenameFilter jsonFileFilter = (dir, name) -> name.endsWith(".json");
        File[] files = folder.listFiles(jsonFileFilter);
        List<String> forexStocksNamesList = new ArrayList<>();
        if (files != null) {
            for (File file : files) {
                forexStocksNamesList.add(file.getAbsolutePath());
            }
        }
        String[] forexStocksNames = forexStocksNamesList.toArray(new String[0]);


        //the topic names
        String topicName1="Data_Topic";
        //properties of the producer
        Producer producer = new Producer();
        producer.produceData(topicName1,forexStocksNames);


    }
}
