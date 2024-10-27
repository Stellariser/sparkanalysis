package com.idooot.bigdata.spark.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import redis.clients.jedis.Jedis;


public class spark2_env {

    public static void main(String[] args) {

        // 1. 创建SparkConf对象
        SparkConf conf = new SparkConf().setAppName("TradeAnalysis").setMaster("local");

        // 2. 创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 读取数据文件
        JavaRDD<String> lines = sc.textFile("data/statistiques-nationales-du-commerce-exterieur.csv");

        // 4. 跳过表头并解析数据
        JavaRDD<String[]> parsedData = lines
                .filter(line -> !line.startsWith("column_1")) // 去掉标题行
                .map(line -> line.split(";")); // 按分号解析CSV数据

        // 5. 计算每个商品的贸易总量和总价值
        JavaPairRDD<String, Tuple2<Double, Double>> tradeVolumeAndValue = parsedData.mapToPair(fields -> {
            String productCode = fields[5]; // 商品编码
            double quantity = Double.parseDouble(fields[7]);
            double value = Double.parseDouble(fields[8]);
            return new Tuple2<>(productCode, new Tuple2<>(quantity, value));
        });

        // 6. 累加每种商品的贸易总量和总价值
        JavaPairRDD<String, Tuple2<Double, Double>> totalTradeVolumeAndValue = tradeVolumeAndValue.reduceByKey(
                (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)
        );
        totalTradeVolumeAndValue.foreachPartition(partition -> {
            try (Jedis jedis = new Jedis("localhost", 6379)) {
                partition.forEachRemaining(data -> {
                    String productCode = data._1;
                    double quantity = data._2._1;
                    double value = data._2._2;
                    jedis.hset("totalTradeVolumeAndValue", productCode, quantity + "," + value);
                });
            }
        });
        // 7. 按商品分组，识别主要的贸易伙伴（国家）
        JavaPairRDD<String, String> productAndCountry = parsedData.mapToPair(fields -> {
            String productCode = fields[5];
            String country = fields[6];
            return new Tuple2<>(productCode, country);
        });

        // 8. 统计每个商品的主要贸易伙伴
        JavaPairRDD<String, Iterable<String>> mainTradePartners = productAndCountry.groupByKey();

        mainTradePartners.foreachPartition(partition -> {
            try (Jedis jedis = new Jedis("localhost", 6379)) {
                partition.forEachRemaining(data -> {
                    String productCode = data._1;
                    Iterable<String> partners = data._2;
                    StringBuilder partnerList = new StringBuilder();
                    for (String partner : partners) {
                        if (partnerList.length() > 0) {
                            partnerList.append(",");
                        }
                        partnerList.append(partner);
                    }
                    jedis.hset("mainTradePartners", productCode, partnerList.toString());
                });
            }
        });

        // 9. 保存总量和总价值结果
        totalTradeVolumeAndValue.saveAsTextFile("data/output/totalTradeVolumeAndValue");

        // 10. 保存主要贸易伙伴结果
        mainTradePartners.saveAsTextFile("data/output/mainTradePartners");

        // 11. 基于时间序列的贸易趋势分析
        JavaPairRDD<String, Tuple2<Double, Double>> timeSeriesAnalysis = parsedData.mapToPair(fields -> {
            String date = fields[2] + "-" + fields[1]; // 年-月
            double quantity = Double.parseDouble(fields[7]);
            double value = Double.parseDouble(fields[8]);
            return new Tuple2<>(date, new Tuple2<>(quantity, value));
        });

        // 12. 按日期汇总每月的贸易量和贸易价值
        JavaPairRDD<String, Tuple2<Double, Double>> monthlyTradeTrends = timeSeriesAnalysis.reduceByKey(
                (v1, v2) -> new Tuple2<>(v1._1 + v2._1, v1._2 + v2._2)
        );

        // 将月度贸易趋势分析结果保存到 Redis
        monthlyTradeTrends.foreachPartition(partition -> {
            try (Jedis jedis = new Jedis("localhost", 6379)) {
                partition.forEachRemaining(data -> {
                    String date = data._1;
                    double quantity = data._2._1;
                    double value = data._2._2;
                    jedis.hset("monthlyTradeTrends", date, quantity + "," + value);
                });
            }
        });

        // 13. 保存月度贸易趋势分析结果
        monthlyTradeTrends.saveAsTextFile("data/output/monthlyTradeTrends");

        // 14. 释放资源
        sc.stop();
    }
}
