package org.kafkaApp.ClassMaybeUsefull;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;


public class TopicCounterTransformSupplier implements ValueTransformerWithKeySupplier<String, Long, Long> {

    private static class TopicCounterTransform implements ValueTransformerWithKey<String, Long,  Long> {
        private KeyValueStore<String, Long> stateStore;
        private static final String Topic_StateStore_COUNTER_KEY = "topic-count";
        @Override
        public void init(ProcessorContext context) {
            stateStore = context.getStateStore("topic-counter-state-store");
        }

        @Override
        public Long transform(String readOnlyKey, Long value) {
            Long currentCount = stateStore.get(Topic_StateStore_COUNTER_KEY);
            System.out.println("1currentCount: "+currentCount);
            if (currentCount == null) {
                currentCount = 0L;
            }
            currentCount += value;  // increment by the value
            System.out.println("2currentCount: "+currentCount);
            stateStore.put(Topic_StateStore_COUNTER_KEY, currentCount);
            return currentCount;
        }


        @Override
        public void close() {

        }

    }
    @Override
    public ValueTransformerWithKey<String, Long,Long> get() {
        return new TopicCounterTransform();
    }
}
