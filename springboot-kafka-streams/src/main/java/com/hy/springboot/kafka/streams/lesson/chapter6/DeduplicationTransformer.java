package com.hy.springboot.kafka.streams.lesson.chapter6;

/**
 * @author hy
 * @date 2022/1/9 8:16 下午
 * @description
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

/**
 * 根据ip地址执行去重逻辑
 * @param <K>
 * @param <V>
 * @param <E>
 */
@Slf4j
public class DeduplicationTransformer<K, V, E> implements Transformer<K, V, KeyValue<K, V>> {

    public static final String storeName = "deduplication";

    private ProcessorContext context;
    private WindowStore<E, Long> eventIdStore;

    private final long leftDurationMs;
    private final long rightDurationMs;

    private final KeyValueMapper<K, V, E> idExtractor;

    DeduplicationTransformer(final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
        if (maintainDurationPerEventInMs < 1) {
            throw new IllegalArgumentException("maintain duration per event must be >= 1");
        }

        leftDurationMs = maintainDurationPerEventInMs / 2;
        rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
        this.idExtractor = idExtractor;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
    }

    @Override
    public KeyValue<K, V> transform(K key, V value) {
        final E eventId = idExtractor.apply(key, value);
        if (eventId == null) {
            return KeyValue.pair(key, value);
        } else {
            final KeyValue<K, V> output;
            if (isDuplicate(eventId)) {
                output = null;
                updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
            } else {
                output = KeyValue.pair(key, value);
                rememberNewEvent(eventId, context.timestamp());
            }
            return output;
        }
    }

    private boolean isDuplicate(final E eventId) {
        final long eventTime = context.timestamp();
        final WindowStoreIterator<Long> timeIterator = eventIdStore.fetch(
                eventId, eventTime - leftDurationMs, eventTime + rightDurationMs);
        final boolean isDuplicate = timeIterator.hasNext();
        timeIterator.close();
        return isDuplicate;
    }

    private void updateTimestampOfExistingEventToPreventExpiry(final E eventId, final long newTimestamp) {
        eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    private void rememberNewEvent(final E eventId, final long timestamp) {
        eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public void close() {

    }
}
