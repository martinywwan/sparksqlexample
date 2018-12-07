package com.martinywwan.launcher;

import com.martinywwan.model.Person;
import com.martinywwan.model.Salary;
import org.apache.avro.Schema;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.stereotype.Component;

import java.io.IOException;

//https://stackoverflow.com/questions/48896452/spark-dataframe-how-to-specify-schema-when-writing-as-avro

@Component
public class SparkSqlExample {


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
                .option("failOnDataLoss", "false")
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
            person.setNamePerson(s);
            int age = (int)(Math.random()*50)+1;
            int v = (int)(Math.random()*100)+1;
            person.setAge(age);
            person.setVersionNumber(v);
            person.setId("id");
           System.out.println("Person table -- name : " + s + " age : " + age + " versionNumber : " + v);
           return person;
        }, personEncoder);
//                .writeStream().format("console").start().processAllAvailable();
        persons.registerTempTable("persons"); //register table first
//        StreamingQuery personQuery = persons.writeStream().format("console").start(); //works
//        personQuery.processAllAvailable(); //works

        /* Once the pojo is mapped , do some querying.. */



        Dataset<String> dataset2 = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "example")
                .option("startingoffsets", "earliest")
                .option("failOnDataLoss", "false")
//                .option("enable.auto.commit", "false")
//                .option("group.id", "mm11")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());
        Encoder<Salary> salaryEncoder = Encoders.bean(Salary.class);
        Dataset<Salary> salaryPerson = dataset2.map((MapFunction<String, Salary>) s -> {
            Salary person = new Salary();
            person.setName(s);
            int number = (int)(Math.random()*40)+1;
            person.setAmount(number);
            System.out.println("Salary table -- name : " + s + " amount : " + number);
            return person;
        }, salaryEncoder);
        salaryPerson.registerTempTable("salary");
//        StreamingQuery exampledata = salaryPerson.writeStream().format("console").start(); //works
//        exampledata.processAllAvailable(); //works


//                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//        StreamingQuery newQuery = dataset2.writeStream().format("console").start();
//        newQuery.processAllAvailable();

//        Dataset<Row> resultset = persons.join(dataset2).where("name == value");

//        salaryPerson.registerTempTable("salary"); //WORKS
//        persons.registerTempTable("persons"); //WORKS

//        StreamingQuery sqlQuery = spark.sql("SELECT * FROM exampledata e JOIN persons p ON e.value = p.name").writeStream().format("console").start(); //This works
//        Dataset sqlds = spark.sql("SELECT * FROM exampledata e JOIN persons p ON e.value = p.name").toDF(); //This works
//        Dataset sqlds = spark.sql("SELECT * FROM exampledata e JOIN persons p ON e.value = p.name").toDF(); //This works
//        /* fields defined in pojo and it is not case sensitive but underscores not allowed if not present*/
//        Dataset sqlds = spark.sql("SELECT * FROM salary s INNER JOIN persons p ON s.name = p.nameperson").toDF(); //This works

        /* after registering table , build query*/
        //https://stackoverflow.com/questions/48533623/how-to-get-a-single-row-with-the-maximum-value-while-keeping-the-whole-row
        Dataset<Row> sqlds = salaryPerson.alias("salaryPerson").join(persons.alias("persons"),
                functions.col("salaryPerson.name").equalTo(functions.col("persons.namePerson"))); //this works!!
        sqlds.writeStream().format("console").start().processAllAvailable(); //This works;
        Schema.Parser parser = new Schema.Parser(); //This works
        Schema schema = parser.parse(ClassLoader.getSystemResourceAsStream("martin.avsc")); //This works
//        System.out.println(schema.toString()); //This works
////        sqlds.write().option("forceSchema",schema.toString()).parquet("hdfs://localhost:9000/user/hive/warehouse/martin.db/martin_test/");
//        StreamingQuery streamingQuery = sqlds.writeStream().option("forceSchema",schema.toString()).option("checkpointLocation","/home/martinywwan/").format("com.databricks.spark.avro").start("hdfs://localhost:9000/user/hive/warehouse/martin.db/camelcase_test");
//        streamingQuery.processAllAvailable();
      //  // in parquet format


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
