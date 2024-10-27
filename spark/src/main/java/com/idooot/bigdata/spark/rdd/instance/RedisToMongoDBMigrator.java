package com.idooot.bigdata.spark.rdd.instance;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RedisToMongoDBMigrator {

    // MongoDB 配置信息
    private static final String MONGO_URI = "mongodb://localhost:27017/";
    private static final String DATABASE_NAME = "spark";

    // MongoDB 集合名称
    private static final String COLLECTION_TOTAL_TRADE = "totalTradeVolumeAndValue";
    private static final String COLLECTION_MAIN_PARTNERS = "mainTradePartners";
    private static final String COLLECTION_MONTHLY_TRENDS = "monthlyTradeTrends";

    // MongoDB 数据库对象
    private MongoDatabase mongoDatabase;

    // 构造方法：连接到 MongoDB
    public RedisToMongoDBMigrator() {
        MongoClient mongoClient = MongoClients.create(MONGO_URI);
        this.mongoDatabase = mongoClient.getDatabase(DATABASE_NAME);
    }
    public void migrateTradeVolumeAndValue() {
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            Map<String, String> data = jedis.hgetAll("totalTradeVolumeAndValue");
            MongoCollection<Document> collection = mongoDatabase.getCollection(COLLECTION_TOTAL_TRADE);

            for (Map.Entry<String, String> entry : data.entrySet()) {
                String productCode = entry.getKey();
                String[] values = entry.getValue().split(",");
                double quantity = Double.parseDouble(values[0]);
                double value = Double.parseDouble(values[1]);

                Document doc = new Document("productCode", productCode)
                        .append("quantity", quantity)
                        .append("value", value);
                collection.insertOne(doc);
            }
        }
    }
    public void migrateMainTradePartners() {
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            Map<String, String> data = jedis.hgetAll("mainTradePartners");
            MongoCollection<Document> collection = mongoDatabase.getCollection(COLLECTION_MAIN_PARTNERS);

            for (Map.Entry<String, String> entry : data.entrySet()) {
                String productCode = entry.getKey();
                // 将 String 转换为 List<String>
                List<String> partners = Arrays.asList(entry.getValue().split(","));

                Document doc = new Document("productCode", productCode)
                        .append("partners", partners);
                collection.insertOne(doc);
            }
        }
    }
    public void migrateMonthlyTradeTrends() {
        try (Jedis jedis = new Jedis("localhost", 6379)) {
            Map<String, String> data = jedis.hgetAll("monthlyTradeTrends");
            MongoCollection<Document> collection = mongoDatabase.getCollection(COLLECTION_MONTHLY_TRENDS);

            for (Map.Entry<String, String> entry : data.entrySet()) {
                String date = entry.getKey();
                String[] values = entry.getValue().split(",");
                double quantity = Double.parseDouble(values[0]);
                double value = Double.parseDouble(values[1]);

                Document doc = new Document("date", date)
                        .append("quantity", quantity)
                        .append("value", value);
                collection.insertOne(doc);
            }
        }
    }
    public static void main(String[] args) {
        RedisToMongoDBMigrator migrator = new RedisToMongoDBMigrator();

        // 调用数据迁移方法
        migrator.migrateTradeVolumeAndValue();
        migrator.migrateMainTradePartners();
        migrator.migrateMonthlyTradeTrends();

        System.out.println("数据已从 Redis 成功迁移到 MongoDB！");
    }






}
