package com.amazonaws.otsmgr.utils;

import java.io.IOException;

import java.util.List;
import java.util.Properties;


import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.stereotype.Component;

import com.amazonaws.otsmgr.beans.MigrationColumn;
import com.amazonaws.otsmgr.beans.MigrationKey;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import jakarta.annotation.PostConstruct;
@Component
public class MongoDBUtil {

    private final Logger log = LoggerFactory.getLogger(MongoDBUtil.class);
    MongoClient mongoClient;
    MongoDatabase database;

    @PostConstruct
    private void init() throws IOException {
        org.springframework.core.io.Resource resource =  new ClassPathResource("config/application.properties");
        Properties properties = PropertiesLoaderUtils.loadProperties(resource);
        mongoClient = MongoClients.create(properties.getProperty("ots.migration.config.mongoDB.url"));
        // Connect to database
        database = mongoClient.getDatabase(properties.getProperty("ots.migration.config.mongoDB.database"));
        log.info("Connected to MongoDB successfully!");
        //TODO 需要加一个用户名密码认证

    }

    public void insertTableItem(String tableName, MigrationKey key, List<MigrationColumn> columns){
        Document document = new Document();
        //设置主键维一值
        document.put("_id", key.getValue());
        columns.forEach(k->{
            document.put(k.getName(), k.getValue());
        });
        database.getCollection(tableName).insertOne(document);
    }

    public void updateTableItem(String tableName,MigrationKey key,List<MigrationColumn> columns){
        MongoCollection<Document> collection = database.getCollection(tableName);
        Document filter = new Document("_id", key.getValue());
        Document updateDoc = new Document();
        columns.forEach(k->{
            updateDoc.put(k.getName(), k.getValue());
        });
        collection.updateOne(filter, new Document("$set", updateDoc));
    }

    public void deleteTableItem(String tableName, MigrationKey key){
        MongoCollection<Document> collection = database.getCollection(tableName);
        Document filter = new Document("_id", key.getValue());
        collection.deleteOne(filter);
    }

    public void cleanTable(String tableName){
        MongoCollection<Document> collection = database.getCollection(tableName);
        collection.drop();
    }


}
