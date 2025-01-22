package org.kafkaApp.Microservices.Router;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeepTrackOfMicroservices {
    private static final String MICROSERVICE_ZNODE = "/activeMicroservices";
    private ZooKeeper zooKeeper;

    public KeepTrackOfMicroservices(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        this.zooKeeper = zooKeeper;
        ensureMapRootExists();
    }

    private void ensureMapRootExists() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(MICROSERVICE_ZNODE, false) == null) {
            zooKeeper.create(MICROSERVICE_ZNODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
    public void put(String key, String value) throws KeeperException, InterruptedException {
        String path = MICROSERVICE_ZNODE + "/" + key;
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } else {
            zooKeeper.setData(path, value.getBytes(), stat.getVersion());
        }
    }
    public String get(String key) throws KeeperException, InterruptedException {
        String path = MICROSERVICE_ZNODE + "/" + key;
        Stat stat = zooKeeper.exists(path, false);
        if (stat != null) {
            byte[] data = zooKeeper.getData(path, false, stat);
            return new String(data);
        }
        return null;
    }
    public void remove(String key) throws KeeperException, InterruptedException {
        String path = MICROSERVICE_ZNODE + "/" + key;
        Stat stat = zooKeeper.exists(path, false);
        if (stat != null) {
            zooKeeper.delete(path, stat.getVersion());
        }
    }
    public Map<String, String> getAllEntries() throws KeeperException, InterruptedException {
        Map<String, String> map = new HashMap<>();
        List<String> keys = zooKeeper.getChildren(MICROSERVICE_ZNODE, false);
        for (String key : keys) {
            String value = get(key);
            map.put(key, value);
        }
        return map;
    }

}
