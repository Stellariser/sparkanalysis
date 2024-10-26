package com.idooot.bigdata.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class sprak1_env {

    public static void main(String[] args) {
        // 1. 创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("SparkEnergyAnalysis").setMaster("local");

        // 2. 创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 读取数据文件
        JavaRDD<String> lines = sc.textFile("data/electricityConsumptionAndProductioction.csv");

        // 4. 跳过表头并解析数据
        JavaRDD<String[]> parsedData = lines
                .filter(line -> !line.startsWith("DateTime")) // 去掉标题行
                .map(line -> line.split(",")); // 解析CSV数据

        // 5. 计算每小时的总生产和总消耗
        JavaPairRDD<String, Tuple2<Double, Double>> consumptionAndProduction = parsedData.mapToPair(fields -> {
            String dateTime = fields[0]; // 获取日期时间
            double consumption = Double.parseDouble(fields[1]);
            double production = Double.parseDouble(fields[2]);
            return new Tuple2<>(dateTime, new Tuple2<>(consumption, production));
        });

        // 6. 汇总结果：计算消耗和生产的总和
        JavaPairRDD<String, Tuple2<Double, Double>> totalConsumptionAndProduction = consumptionAndProduction.reduceByKey(new Function2<Tuple2<Double, Double>, Tuple2<Double, Double>, Tuple2<Double, Double>>() {
            @Override
            public Tuple2<Double, Double> call(Tuple2<Double, Double> v1, Tuple2<Double, Double> v2) {
                return new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2); // 累加消耗和生产
            }
        });

        // 7. 将结果保存到文件
        totalConsumptionAndProduction.saveAsTextFile("data/output/totalConsumptionAndProduction");

        // 8. 释放资源
        sc.stop();
    }


}
