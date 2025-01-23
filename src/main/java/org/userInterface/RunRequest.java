package org.userInterface;

import org.kafkaApp.Configuration.EnvironmentConfiguration;

public class RunRequest {
    public static void main(String[] args)  {
        //the request path of the data, wanted to send to the kafka
        String RequestPath1= EnvironmentConfiguration.getFilePathForRequestTopic();

        //the topic names
        String topicName2="Request_Topic";

        //properties of the producerData
        Producer producerData = new Producer();

        producerData.produceRequest(topicName2,RequestPath1);
    }
}
