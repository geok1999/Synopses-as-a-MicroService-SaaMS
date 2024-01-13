package org.kafkaApp.InitProducer;

public class RunRequest {
    public static void main(String[] args)  {
        //the request path of the data, wanted to send to the kafka
        String RequestPath1="C:/Request_small.json";

        //the topic names
        String topicName2="Request_Topic";

        //properties of the producer
        Producer producer = new Producer();

        producer.produceRequest(topicName2,RequestPath1);
    }
}
