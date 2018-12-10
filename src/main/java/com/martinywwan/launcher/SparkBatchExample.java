package com.martinywwan.launcher;

import io.netty.handler.codec.string.StringDecoder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka010.*;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class SparkBatchExample {

    OffsetRange[] offsetRanges = {
            // topic, partition, inclusive starting offset, exclusive ending offset
            OffsetRange.create("test", 0, 0, 100),
            OffsetRange.create("example", 1, 0, 100)
    };


    public void run() throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("hello");
//        printStreamContent(sparkConf);
        runBatch(sparkConf);
    }

    private void runBatch(SparkConf sparkConf) throws InterruptedException {
        AtomicReference<List<OffsetRange>> offsetRanges = getOffsets(sparkConf);
        System.out.println("SIZE OF OFFSET " + Arrays.asList(offsetRanges.get()).size());
        offsetRanges.get().forEach(o -> System.out.println("VALUE " + o.topic()));
//        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
//        System.out.println("Starting batch");
//        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(sc, getKafkaParam(), ()offsetRanges.get().toArray(), LocationStrategies.PreferConsistent());
//        for (ConsumerRecord<String, String> line : rdd.collect()) {
//            System.out.println("FINAL RESULT:: " + line.topic() + " :: " + line.value());
//        }
    }


    private AtomicReference<List<OffsetRange>> getOffsets(SparkConf sparkConf) throws InterruptedException {
        final AtomicReference<List<OffsetRange>> atomicReference = new AtomicReference<>();

        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Duration.apply(50));
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(sc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList("test"), getKafkaParam())
                );
        stream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
                    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    final List<OffsetRange> list = new ArrayList<>();
                    rdd.foreachPartition((VoidFunction<Iterator<ConsumerRecord<String, String>>>) consumerRecords -> {
                        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                        list.add(o);
                        atomicReference.set(list);
                        System.out.println(
                                o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                    });
                  sc.stop();
                }
        );
        sc.start();
        sc.awaitTermination();
        System.out.println("SIZE OF initial OFFSET " + Arrays.asList(atomicReference.get()).size());

//        sc.stop();
        return atomicReference;
    }

    private void printStreamContent(SparkConf sparkConf) throws InterruptedException {
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Duration.apply(50));
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(sc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList("test"), getKafkaParam())
                );
        stream.foreachRDD(call -> call.foreach(call2 -> System.out.println("topic : " + call2.topic() + " value : " + call2.value())));
        sc.start();
    }

    private Map<String, Object> getKafkaParam() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "stefstef1");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return kafkaParams;
    }

//    public class OffsetResult implements Callable<AtomicReference<OffsetRange[]>> {
//
//        @Override
//        public AtomicReference<OffsetRange[]> call() throws Exception {
//            final AtomicReference<OffsetRange[]> atomicReference = new AtomicReference<OffsetRange[]>();
//            SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("hello");
//            JavaStreamingContext sc = new JavaStreamingContext(sparkConf, Duration.apply(50));
//            JavaInputDStream<ConsumerRecord<String, String>> stream =
//                    KafkaUtils.createDirectStream(sc,
//                            LocationStrategies.PreferConsistent(),
//                            ConsumerStrategies.<String, String>Subscribe(Arrays.asList("test"), getKafkaParam())
//                    );
//            stream.foreachRDD((VoidFunction<JavaRDD<ConsumerRecord<String, String>>>) rdd -> {
//                        final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//                        rdd.foreachPartition((VoidFunction<Iterator<ConsumerRecord<String, String>>>) consumerRecords -> {
//                            OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//                            atomicReference.set(offsetRanges);
//                            System.out.println(
//                                    o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
//                        });
//                        throw new InterruptedException();
//                    }
//            );
//            sc.start();
//            sc.awaitTermination();
//            return atomicReference;
//        }

}
