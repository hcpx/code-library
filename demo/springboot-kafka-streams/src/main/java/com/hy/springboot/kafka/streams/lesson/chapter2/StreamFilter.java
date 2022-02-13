package com.hy.springboot.kafka.streams.lesson.chapter2;

import com.google.common.collect.Lists;
import com.hy.springboot.kafka.streams.domain.Publication;
import com.hy.springboot.kafka.streams.domain.RawMovie;
import com.hy.springboot.kafka.streams.lesson.common.abs.AbstractStreamProducer;
import java.util.List;
import javax.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author HY
 * @version 0.0.1
 * @date 2021/2/1 11:01
 */
@Slf4j
@Configuration("streamImpl")
public class StreamFilter extends AbstractStreamProducer<Publication>{

    @Bean
    public KStream<String, Publication> mapStream() {
        KStream<String, Publication> toStream = streamsBuilder.<String, Publication>stream(getInputTopicName())
                .filter((k, v) -> "George R. R. Martin".equals(v.getName()));
        toStream.peek((k, v) -> log.info("after filter:{}", v));
        return toStream;
    }

    @Override
    public String getInputTopicName() {
        return getClassName(Publication.class);
    }

    @Override
    public List<Publication> getData() {
        return Lists.newArrayList(
                Publication.builder()
                        .name("George R. R. Martin").title("A Song of Ice and Fire").build(),
                Publication.builder()
                        .name("C.S. Lewis").title("The Silver Chair").build(),
                Publication.builder()
                        .name("C.S. Lewis").title("Perelandra").build(),
                Publication.builder()
                        .name("George R. R. Martin").title("Fire & Blood").build(),
                Publication.builder()
                        .name("J. R. R. Tolkien").title("The Hobbit").build(),
                Publication.builder()
                        .name("J. R. R. Tolkien").title("The Lord of the Rings").build(),
                Publication.builder()
                        .name("George R. R. Martin").title("A Dream of Spring").build(),
                Publication.builder()
                        .name("J. R. R. Tolkien").title("The Fellowship of the Ring").build(),
                Publication.builder()
                        .name("George R. R. Martin").title("The Ice Dragon").build()
        );
    }
}
