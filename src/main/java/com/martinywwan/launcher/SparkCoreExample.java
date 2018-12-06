//package com.martinywwan.launcher;
//
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.SparkConf;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.Map;
//
//public class SparkCoreExample {
//
//    public void run() throws InterruptedException {
//        String topic1 = "martin";
//        String topic2 = "test";
//        SparkConf sparkConf = new SparkConf().setAppName("martinywwan app").setMaster("spark://martinywwan-Inspiron-7570:7077");
////        SparkConf sparkConf = new SparkConf().setAppName("martinywwan app").setMaster("local[*]");
//        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "testId8");
//        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//
//        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.Subscribe(Arrays.asList(topic1), kafkaParams)); //StreamingContext object has to be created which is the main entry point of all Spark Streaming functionality.
//        stream.map(x -> topic1+"--"+x.value()).print();
//
//        JavaInputDStream<ConsumerRecord<String, String>> stream2 = KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(),
//                ConsumerStrategies.Subscribe(Arrays.asList(topic2), kafkaParams)); //StreamingContext object has to be created which is the main entry point of all Spark Streaming functionality.
//        stream2.map(x -> topic2+"--"+x.value()).print();
//
//        jssc.start();
//        jssc.awaitTermination();
//    }
//}
