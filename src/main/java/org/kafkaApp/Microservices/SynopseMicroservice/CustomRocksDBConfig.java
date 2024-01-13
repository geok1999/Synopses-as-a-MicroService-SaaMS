package org.kafkaApp.Microservices.SynopseMicroservice;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;

public class CustomRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        options.setMaxOpenFiles(4000);
        options.setNumLevels(4);
        options.setMaxBytesForLevelBase(512 * 1024 * 1024);
        options.setMaxBytesForLevelMultiplier(5);
        options.setRecycleLogFileNum(2);
    }

    @Override
    public void close(String storeName, Options options) {

    }
}
