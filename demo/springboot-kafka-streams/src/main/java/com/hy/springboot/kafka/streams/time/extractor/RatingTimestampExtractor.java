package com.hy.springboot.kafka.streams.time.extractor;

import com.hy.springboot.kafka.streams.domain.Rating;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * @author hy
 * @date 2022/1/14 12:14 下午
 * @description
 */
public class RatingTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Object value = record.value();
        if (value instanceof Rating) {
            final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String eventTime = ((Rating) value).getTimestamp();
            try {
                return sdf.parse(eventTime).getTime();
            } catch (ParseException e) {
                return 0;
            }
        }
        return System.currentTimeMillis();
    }
}
