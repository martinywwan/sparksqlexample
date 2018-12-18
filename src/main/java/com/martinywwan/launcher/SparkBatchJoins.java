package com.martinywwan.launcher;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class SparkBatchJoins {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("hello");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        Class<?>[] classes = new Class[]{ Person.class, Vehicle.class};
        sparkConf.registerKryoClasses(classes);
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
        Person person = Person.builder().name("martin").age(1).version(2).orderId("3").build();
        Person person2 = Person.builder().name("stef").age(2).version(1).orderId("2").build();

        Vehicle vehicle = Vehicle.builder().vehicleName("car").carVersion(2).orderId("3").build();
        Vehicle vehicle2 = Vehicle.builder().vehicleName("car2").carVersion(2).orderId("4").build();
        Vehicle vehicle3 = Vehicle.builder().vehicleName("car3").carVersion(1).orderId("4").build();
        Vehicle vehicle4 = Vehicle.builder().vehicleName("car4").carVersion(2).orderId("3").build();

        JavaRDD<Person> personJavaRDD = sc.parallelize(Arrays.asList(person, person2));
        JavaRDD<Vehicle> vehicleJavaRDD = sc.parallelize(Arrays.asList(vehicle, vehicle2, vehicle3, vehicle4));

        JavaPairRDD<Tuple2<String, Integer>, Person> personJavaPairRDD = personJavaRDD.mapToPair((PairFunction<Person, Tuple2<String, Integer>, Person>) p ->
                Tuple2.apply(Tuple2.apply(p.getOrderId(), p.getVersion()), p));

        JavaPairRDD<Tuple2<String, Integer>, Vehicle> vehicleJavaPairRDD = vehicleJavaRDD.mapToPair((PairFunction<Vehicle, Tuple2<String, Integer>, Vehicle>) p ->
                Tuple2.apply(Tuple2.apply(p.getOrderId(), p.getCarVersion()), p)).reduceByKey((Function2<Vehicle, Vehicle, Vehicle>) (vehicle1, vehicle21) -> vehicle1);

        personJavaPairRDD.leftOuterJoin(vehicleJavaPairRDD).foreach(new VoidFunction<Tuple2<Tuple2<String, Integer>, Tuple2<Person, Optional<Vehicle>>>>() {
            @Override
            public void call(Tuple2<Tuple2<String, Integer>, Tuple2<Person, Optional<Vehicle>>> tuple2Tuple2Tuple2) throws Exception {
                System.out.println("*************OrderID : " + tuple2Tuple2Tuple2._1._1 + " Version : " + tuple2Tuple2Tuple2._1._2 + " \n "+
                "Person : " + tuple2Tuple2Tuple2._2._1.toString() + "\n" +
                "vehicle : " + (tuple2Tuple2Tuple2._2._2.isPresent()? tuple2Tuple2Tuple2._2._2.get().toString() : null));

            }
        });
    }


    @Getter
    @Setter
    @Builder
    public static class Person {

        private String name;

        private Integer age;

        private Integer version;

        private String orderId;

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", version=" + version +
                    ", orderId='" + orderId + '\'' +
                    '}';
        }
    }


    @Getter
    @Setter
    @Builder
    public static class Vehicle {

        private String vehicleName;

        private Integer carVersion;

        private String orderId;

        @Override
        public String toString() {
            return "Vehicle{" +
                    "vehicleName='" + vehicleName + '\'' +
                    ", carVersion=" + carVersion +
                    ", orderId='" + orderId + '\'' +
                    '}';
        }
    }
}
