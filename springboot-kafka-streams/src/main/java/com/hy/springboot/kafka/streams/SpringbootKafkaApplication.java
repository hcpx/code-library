package com.hy.springboot.kafka.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter1"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter2"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter3"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter4"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter5"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter6"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter7"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter8"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter9"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter10"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter11"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter12"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter13"})
@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter14"})
//@SpringBootApplication(scanBasePackages = {"com.hy.springboot.kafka.streams.lesson.common","com.hy.springboot.kafka.streams.lesson.chapter15"})
//@SpringBootApplication
@EnableScheduling
@EnableKafkaStreams
public class SpringbootKafkaApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootKafkaApplication.class, args);
    }

}
