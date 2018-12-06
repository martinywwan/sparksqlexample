package com.martinywwan;

import com.martinywwan.launcher.SparkSqlExample;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

import java.io.IOException;

@ComponentScan
public class Application {

    public static void main(String[] args) throws StreamingQueryException, IOException {
        ApplicationContext ac = new AnnotationConfigApplicationContext(Application.class);
        ac.getBean(SparkSqlExample.class).run();
    }
}
