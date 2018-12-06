package com.martinywwan.launcher;

import com.martinywwan.model.Person;
import org.apache.avro.Schema;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

//https://stackoverflow.com/questions/48896452/spark-dataframe-how-to-specify-schema-when-writing-as-avro

@Component
public class SparkSqlExample {

    public static AtomicInteger inc = new AtomicInteger(1);

    public static AtomicBoolean boo = new AtomicBoolean(false);

    public void run() throws StreamingQueryException, IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
//                .config("spark.some.config.option", "some-value")
                .master("local[*]")
                .getOrCreate();
        Dataset<String> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "test")
                .option("startingoffsets", "earliest")
//                .option("enable.auto.commit", "false")
//                .option("group.id", "mm11")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());
//                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");


        Encoder<Person> personEncoder = Encoders.bean(Person.class);

        /* MAP TO POJO AFTER READING FROM KAFKA */
       Dataset<Person> persons = df.map((MapFunction<String, Person>) s -> {
            Person person = new Person();
            person.setName(s);
            person.setAge(inc.getAndIncrement());
            person.setVersion(boo.getAndSet(boo.get()?  false: true)? 1 : 0);
            return person;
        }, personEncoder);
//                .writeStream().format("console").start().processAllAvailable();
        StreamingQuery personQuery = persons.writeStream().format("console").start();
        personQuery.processAllAvailable();

        /* Once the pojo is mapped , do some querying.. */



        Dataset<String> dataset2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "example")
                .option("startingoffsets", "earliest")
//                .option("enable.auto.commit", "false")
//                .option("group.id", "mm11")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());
        StreamingQuery exampledata = dataset2.writeStream().format("console").start();
        exampledata.processAllAvailable();

//                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//        StreamingQuery newQuery = dataset2.writeStream().format("console").start();
//        newQuery.processAllAvailable();

//        Dataset<Row> resultset = persons.join(dataset2).where("name == value");

        dataset2.registerTempTable("exampledata");
        persons.registerTempTable("persons");

//        StreamingQuery sqlQuery = spark.sql("SELECT * FROM exampledata e JOIN persons p ON e.value = p.name").writeStream().format("console").start(); //This works
//        Dataset sqlds = spark.sql("SELECT * FROM exampledata e JOIN persons p ON e.value = p.name").toDF(); //This works
//        Dataset sqlds = spark.sql("SELECT * FROM exampledata e JOIN persons p ON e.value = p.name").toDF(); //This works
        Dataset sqlds = spark.sql("SELECT * FROM exampledata e JOIN persons p ON e.value = p.name").toDF(); //This works
        sqlds.writeStream().format("console").start().processAllAvailable(); //This works;

        Schema.Parser parser = new Schema.Parser(); //This works
        Schema schema = parser.parse(ClassLoader.getSystemResourceAsStream("martin.avsc")); //This works
        System.out.println(schema.toString()); //This works
//        sqlds.write().option("forceSchema",schema.toString()).parquet("hdfs://localhost:9000/user/hive/warehouse/martin.db/martin_test/");
        StreamingQuery streamingQuery = sqlds.writeStream().option("forceSchema",schema.toString()).option("checkpointLocation","/home/martinywwan/").start("hdfs://localhost:9000/user/hive/warehouse/martin.db/martin_test/");
        streamingQuery.processAllAvailable();
        // in parquet format


//        sqlds.toDF().write().option("forceSchema",schema.toString()).parquet().save("hdfs://localhost:9000/user/hive/warehouse/martin.db/martin_test/");
//                .write().option("forceSchema","").save("hdfs://localhost:9000/user/hive/warehouse/martin.db/martin_test/");
        //df.write.option("forceSchema", myCustomSchemaString).avro("/path/to/outputDir")
//        sqlQuery.processAllAvailable();
//        resultQry.show();
//        df.printSchema();
//        StreamingQuery streamingQuery = df.writeStream().format("console").start();
//        streamingQuery.processAllAvailable();
    }


}
