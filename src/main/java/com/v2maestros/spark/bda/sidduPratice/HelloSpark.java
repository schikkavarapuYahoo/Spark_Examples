package com.v2maestros.spark.bda.sidduPratice;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class HelloSpark {
    public static void main(String[] args) {

       String appName = "HelloSpark";
        String sparkMaster = "local[2]";

       JavaSparkContext sparkContext = null;

       SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);

       sparkContext = new JavaSparkContext(conf);
       //Loading the data into the spark context
       JavaRDD<String> javaRDD = sparkContext.textFile("data/movietweets.csv");

        for (String s: javaRDD.take(5)) {
            System.out.println(s);
        }

        System.out.println("Total Tweet: "+ javaRDD.count());

        while (true){
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



    }
}
