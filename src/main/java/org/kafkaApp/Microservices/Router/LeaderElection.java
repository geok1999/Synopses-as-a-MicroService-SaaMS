package org.kafkaApp.Microservices.Router;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.kafkaApp.Configuration.EnvironmentConfiguration;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class LeaderElection {
    private static final String ZOOKEEPER_ADDRESS = EnvironmentConfiguration.getZookeeperBoostrapServer();//"localhost:2181";
    private static final String LEADER_PATH = "/leader";
    private static final String INSTANCES_PATH = "/instances";
    private static final String COUNTER_ZNODE = "/counter";
    private ZooKeeper zooKeeper;
    private static final String INSTANCE_ID = generateInstanceId();

    public void ensureCounterNodeExists() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(COUNTER_ZNODE, false) == null) {
            zooKeeper.create(COUNTER_ZNODE, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public int incrementAndGet() throws Exception {
        while (true) {
            Stat stat = new Stat();
            byte[] data = zooKeeper.getData(COUNTER_ZNODE, false, stat);
            int currentValue = Integer.parseInt(new String(data));
            int newValue = currentValue + 1;

            try {
                zooKeeper.setData(COUNTER_ZNODE, String.valueOf(newValue).getBytes(), stat.getVersion());
                return newValue;
            } catch (Exception ex) {
                // Handle version conflicts or other issues and retry
            }
        }
    }
    private static String generateInstanceId() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            return hostname + "_" + timestamp;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate instance ID", e);
        }
    }

    public String getInstanceName() {
        return INSTANCE_ID;
    }


    public void connectToZooKeeper() throws Exception {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, 3000, watchedEvent -> {
            if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted ||
                    watchedEvent.getState() == Watcher.Event.KeeperState.Expired) {
                try {
                    electLeader();
                } catch (InterruptedException | KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        if (zooKeeper.exists(INSTANCES_PATH, false) == null) {
            zooKeeper.create(INSTANCES_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        zooKeeper.create(INSTANCES_PATH + "/" + INSTANCE_ID, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // Ensure the counter znode exists
        ensureCounterNodeExists();
    }


    public void electLeader() throws InterruptedException, KeeperException {
        try {
            zooKeeper.create(LEADER_PATH, INSTANCE_ID.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("I am the leader!");
        } catch (KeeperException.NodeExistsException e) {
            System.out.println("Another instance is the leader.");
        } catch (Exception e) {
            Stat stat = zooKeeper.exists(LEADER_PATH, true);
            if (stat == null) {
                electLeader();
            }
        }
    }

    public boolean isLeader() {
        try {
            Stat stat = zooKeeper.exists(LEADER_PATH, false);
            if (stat != null) {
                byte[] data = zooKeeper.getData(LEADER_PATH, false, stat);
                if (INSTANCE_ID.equals(new String(data))) {
                    System.out.println("I am the leader!");
                    return true;
                } else {
                    System.out.println("I am not the leader.");
                    return false;
                }
            } else {
                System.out.println("I am not the leader.");
                return false;
            }
        } catch (KeeperException.NoNodeException e) {
            System.out.println("I am not the leader.");
            return false;
        } catch (Exception e) {
            System.out.println("Error occurred while checking leader status.");
            return false;
        }
    }
    public int getActiveInstanceCount() {
        try {
            return zooKeeper.getChildren(INSTANCES_PATH, false).size();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get active instance count", e);
        }
    }
    public List<String> getActiveInstances() {
        try {
            return zooKeeper.getChildren(INSTANCES_PATH, false);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get active instances", e);
        }
    }

    public void close() throws InterruptedException {
        if (zooKeeper != null) {
            zooKeeper.close();
        }
    }

}
