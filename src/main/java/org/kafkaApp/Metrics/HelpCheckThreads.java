package org.kafkaApp.Metrics;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class HelpCheckThreads {
    public static int getNumberOfLiveThreads() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        return threadMXBean.getThreadCount();
    }
    public static void main(String[] args){
        int liveThreads = getNumberOfLiveThreads();
        System.out.println("Number of live threads: " + liveThreads);
    }
}
