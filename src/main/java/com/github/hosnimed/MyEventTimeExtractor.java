package com.github.hosnimed;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyEventTimeExtractor implements TimestampExtractor {
    // Extracts the embedded timestamp of a record (giving you "event-time" semantics).
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        long ts = -1;
        final Foo pojo = (Foo) record.value();
        if(pojo != null) {
            ts = pojo.getTS();
            if(ts < 0){
                // Invalid timestamp!  Attempt to estimate a new timestamp,
                // otherwise fall back to wall-clock time (processing-time).
                if(previousTimestamp >= 0){
                    return previousTimestamp;
                }else{
                    return System.currentTimeMillis();
                }
            }
        }
        return ts;
    }
}

class Foo {
    long ts = 0L;
    long getTS()  { return this.ts ; }
}