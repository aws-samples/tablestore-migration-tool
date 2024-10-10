// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.plugin;

import org.springframework.util.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.amazonaws.otsmgr.beans.MigrationColumn;
import com.amazonaws.otsmgr.beans.MigrationKey;
import com.amazonaws.otsmgr.beans.MigrationTable;
import com.amazonaws.otsmgr.conf.MigrationConfig;
import com.amazonaws.otsmgr.utils.MongoDBUtil;
import com.amazonaws.otsmgr.utils.OTSUtil;
import jakarta.annotation.Resource;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

@Component
public class Migration2MongoDB extends MigrationPluginParent{


    private static final Logger log = LoggerFactory.getLogger(Migration2MongoDB.class);
    @Resource
    private OTSUtil otsUtil;
    @Resource
    private MongoDBUtil mongodbUtil;
    @Resource
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;


    @Override
    public void migrate(MigrationConfig config) {
        log.info("Start migrating TableStore to MongoDB.");
        List<MigrationTable> sourceTables = sourceTables();
        List<MigrationTable> _sourceTables = new ArrayList<>();
        if(!CollectionUtils.isEmpty(config.getTableNames())) {
            _sourceTables = sourceTables.stream().filter(t ->
                CollectionUtils.contains(config.getTableNames().iterator(), t.getTableMeta().getTableName())
            ).collect(Collectors.toList());
        }
        clean(config, _sourceTables);
        //migrateSchema(config, _sourceTables);
        try {
            migrateData(config, _sourceTables);
        } catch (IOException e) {
            log.error("Can't load properties file", e);
        }
    }

    private void clean(MigrationConfig config, List<MigrationTable> sourceTables) {
        if(config.isRestart()) {
            for (MigrationTable migrationTable : sourceTables) {
                otsUtil.deleteTunel(migrationTable.getTableMeta().getTableName());
                mongodbUtil.cleanTable(migrationTable.getTableMeta().getTableName());
            }
        }
    }

    private void migrateData(MigrationConfig config, List<MigrationTable> sourceTables) throws IOException {
        org.springframework.core.io.Resource resource =  new ClassPathResource("config/application.properties");
        Properties properties = PropertiesLoaderUtils.loadProperties(resource);
        for (MigrationTable migrationTable : sourceTables) {
            String tableName = migrationTable.getTableMeta().getTableName();
            String primaryKey = properties.getProperty("ots.migration.config.tablePK."+tableName); 
            log.info("Started migrating TableStore data to MongoDB, " + tableName);
            Runnable runnable = otsUtil.getRunner(new MongoDBProcessor(tableName,primaryKey), tableName);
            threadPoolTaskExecutor.execute(runnable);
            log.info("Finished migrating TableStore data to MongoDB, " + tableName);
        }
    }


    @Override
    public boolean supports(MigrationTargetEnum delimiter) {
        return delimiter == MigrationTargetEnum.MongoDB;
    }

    private void operateInMongoDB(String tableName,String primaryKey, StreamRecord r){

        switch ((r.getRecordType())) {
            case PUT:
                putInMongoDB(tableName,primaryKey, r);  
                break;
            case UPDATE:
                updateTableItem(tableName,primaryKey, r); 
                break;
            case DELETE:
                deleteInMongoDB(tableName,primaryKey,r);
                break;
        
        }

    }
    /**
     * @param tableName
     * @param primaryKey
     * @param r
     */
    private void putInMongoDB(String tableName,String primaryKey, StreamRecord r){
        List<MigrationColumn> columns = Arrays.stream(r.getPrimaryKey().getPrimaryKeyColumns()).map(k -> {
            MigrationColumn c = new MigrationColumn();
            c.setName(k.getName());
            //c.setType(k.getValue().getType());
            c.setValue(k.getValue().toString());
            return c;
        }).collect(Collectors.toList());

        //获取主键值
        MigrationKey key = new MigrationKey();
        columns.forEach(k->{
            if(k.getName().equals(primaryKey)){
                key.setName(k.getName());
                key.setValue(k.getValue());
            }
        });
        columns.addAll(r.getColumns().stream().map(k ->{
            MigrationColumn c = new MigrationColumn();
            c.setName(k.getColumn().getName());
            c.setType(k.getColumn().getValue().getType());
            c.setValue(k.getColumn().getValue().toString());
            return c;
        }).collect(Collectors.toList()));

        mongodbUtil.insertTableItem(tableName, key, columns);

    }

    private void updateTableItem(String tableName,String primaryKey, StreamRecord r){
        List<MigrationColumn> columns = Arrays.stream(r.getPrimaryKey().getPrimaryKeyColumns()).map(k -> {
            MigrationColumn c = new MigrationColumn();
            c.setName(k.getName());
            //c.setType(k.getValue().getType());
            c.setValue(k.getValue().toString());
            return c;
        }).collect(Collectors.toList());
        //获取主键值
        MigrationKey key = new MigrationKey();
        columns.forEach(k->{
            if(k.getName().equals(primaryKey)){
                key.setName(k.getName());
                key.setValue(k.getValue());
            }
        });
        columns.addAll(r.getColumns().stream().map(k ->{
            MigrationColumn c = new MigrationColumn();
            c.setName(k.getColumn().getName());
            c.setType(k.getColumn().getValue().getType());
            c.setValue(k.getColumn().getValue().toString());
            return c;
        }).collect(Collectors.toList()));
        mongodbUtil.updateTableItem(tableName, key, columns); 
    }

    private void deleteInMongoDB(String tableName,String primaryKey, StreamRecord r){
        List<MigrationColumn> columns = Arrays.stream(r.getPrimaryKey().getPrimaryKeyColumns()).map(k -> {
            MigrationColumn c = new MigrationColumn();
            c.setName(k.getName());
            //c.setType(k.getValue().getType());
            c.setValue(k.getValue().toString());
            return c;
        }).collect(Collectors.toList());
        //获取主键值
        MigrationKey key = new MigrationKey();
        columns.forEach(k->{
            if(k.getName().equals(primaryKey)){
                key.setName(k.getName());
                key.setValue(k.getValue());
            }
        });

        mongodbUtil.deleteTableItem(tableName, key);


    }

    class MongoDBProcessor implements IChannelProcessor {
        private String tableName = null;
        private String primaryKey = null;

        public MongoDBProcessor(String tableName,String primaryKey) {
            this.tableName = tableName;
            this.primaryKey = primaryKey;
        }
        @Override
        public void process(ProcessRecordsInput input) {
            //ProcessRecordsInput中包含有拉取到的数据。
            log.info("Default record processor, would print records count");
            //NextToken用于Tunnel Client的翻页。
            log.info(String.format("Process %d records, NextToken: %s", input.getRecords().size(), input.getNextToken()));
            
            for (StreamRecord r : input.getRecords()) {
                try{
                    operateInMongoDB(tableName,primaryKey, r);
                }catch(Exception e){
                    log.error("send to MongoDB failed with record: " + r.toString(), e);
                }
                
            }
        }

        @Override
        public void shutdown() {
            log.info("process shutdown du to finished for table: " + tableName);
        }
    }
    
}
