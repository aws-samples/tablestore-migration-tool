// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.plugin;

import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.amazonaws.otsmgr.beans.MigrationColumn;
import com.amazonaws.otsmgr.beans.MigrationKey;
import com.amazonaws.otsmgr.conf.MigrationConfig;
import com.amazonaws.otsmgr.beans.MigrationTable;
import com.amazonaws.otsmgr.utils.DynamoDBUtil;
import com.amazonaws.otsmgr.utils.OTSUtil;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class Migration2DynamoDB extends MigrationPluginParent {
    private static final Logger log = LoggerFactory.getLogger(Migration2DynamoDB.class);

    @Resource
    private DynamoDBUtil dynamoDBUtil;

    @Resource
    private OTSUtil otsUtil;

    @Resource
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm");

    @Override
    public void migrate(MigrationConfig config) {
        log.info("Started migrating TableStore to DynamoDB.");
        List<MigrationTable> sourceTables = sourceTables();
        List<MigrationTable> _sourceTables = new ArrayList<>();
        if(!CollectionUtils.isEmpty(config.getTableNames())) {
            _sourceTables = sourceTables.stream().filter(t ->
                CollectionUtils.contains(config.getTableNames().iterator(), t.getTableMeta().getTableName())
            ).collect(Collectors.toList());
        }
        clean(config, _sourceTables);
        migrateSchema(config, _sourceTables);
        migrateData(config, _sourceTables);
        log.info("Finished migrating TableStore to DynamoDB.");
    }

    public void clean(MigrationConfig config, List<MigrationTable> sourceTables) {
        if(config.isRestart()) {
            for (MigrationTable migrationTable : sourceTables) {
                otsUtil.deleteTunel(migrationTable.getTableMeta().getTableName());
                dynamoDBUtil.deleteTable(migrationTable.getTableMeta().getTableName());
            }
        }
    }

    @Override
    public boolean supports(MigrationTargetEnum migrationTarget) {
        return migrationTarget == MigrationTargetEnum.DynamoDB;
    }

    private void migrateSchema(MigrationConfig config, List<MigrationTable> sourceTables) {
        if(TargetDynamoDBType.migrateSchema(config.getTargetDynamodbType())) {
            for (MigrationTable migrationTable : sourceTables) {
                log.info("Started migrating TableStore schema to DynamoDB, " + migrationTable.getTableMeta().getTableName());
                dynamoDBUtil.createTable(migrationTable);
                log.info("Finished migrating TableStore schema to DynamoDB, " + migrationTable.getTableMeta().getTableName());
            }
        }
    }

    private void migrateData(MigrationConfig config, List<MigrationTable> sourceTables) {
        if(TargetDynamoDBType.migrateData(config.getTargetDynamodbType())) {
            for (MigrationTable migrationTable : sourceTables) {
                String tableName = migrationTable.getTableMeta().getTableName();
                log.info("Started migrating TableStore data to DynamoDB, " + tableName);
                Runnable runnable = otsUtil.getRunner(new DynamoDBProcessor(tableName), tableName);
                threadPoolTaskExecutor.execute(runnable);
                log.info("Finished migrating TableStore data to DynamoDB, " + tableName);
            }
        }
    }

    private void operateInDynanoDB(String tableName, StreamRecord r) {
        switch (r.getRecordType()) {
            case PUT:
            case UPDATE:
                putInDynamoDB(tableName, r);
                break;

            case DELETE:
                deleteInDynamoDB(tableName, r);
                break;
        }
    }

    private void deleteInDynamoDB(String tableName, StreamRecord r) {
        List<MigrationKey> keys = new ArrayList<>();
        for(PrimaryKeyColumn k : r.getPrimaryKey().getPrimaryKeyColumns()) {
            MigrationKey c = new MigrationKey();
            c.setName(k.getName());
            c.setType(k.getValue().getType());
            c.setValue(k.getValue().toString());
            keys.add(c);
        }

        dynamoDBUtil.deleteTableItem(tableName, keys);
    }

    private void putInDynamoDB(String tableName, StreamRecord r) {
        List<MigrationKey> keys = Arrays.stream(r.getPrimaryKey().getPrimaryKeyColumns()).limit(2).map(k -> {
            MigrationKey c = new MigrationKey();
            c.setName(k.getName());
            c.setType(k.getValue().getType());
            c.setValue(k.getValue().toString());
            return c;
        }).collect(Collectors.toList());

        List<MigrationColumn> columns = Arrays.stream(r.getPrimaryKey().getPrimaryKeyColumns()).skip(2).map( k -> {
            MigrationColumn c = new MigrationColumn();
            PrimaryKeyType _type = k.getValue().getType();
            c.setName(k.getName());
            c.setType(ColumnType.valueOf(_type.name()));
            c.setValue(k.getValue().toString());
            return c;
        }).collect(Collectors.toList());

        columns.addAll(r.getColumns().stream().map( k -> {
            MigrationColumn c = new MigrationColumn();
            c.setName(k.getColumn().getName());
            c.setType(k.getColumn().getValue().getType());
            c.setValue(k.getColumn().getValue().toString());
            return c;
        }).collect(Collectors.toList()));

        if(r.getRecordType() == StreamRecord.RecordType.PUT) {
            dynamoDBUtil.insertTableItem(tableName, keys, columns);
        }

        else if(r.getRecordType() == StreamRecord.RecordType.UPDATE) {
            dynamoDBUtil.updateTableItem(tableName, keys, columns);
        }
    }

    class DynamoDBProcessor implements IChannelProcessor {
        private String tableName = null;

        public DynamoDBProcessor(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public void process(ProcessRecordsInput input) {
            //ProcessRecordsInput中包含有拉取到的数据。
            log.info("Default record processor, would print records count");
            //NextToken用于Tunnel Client的翻页。
            log.info(String.format("Process %d records, NextToken: %s", input.getRecords().size(), input.getNextToken()));

            for(StreamRecord r : input.getRecords()) {
                try {
                    operateInDynanoDB(tableName, r);
                }
                catch (Exception e) {
                    log.error("send to DynamoDB failed with record: " + r.toString(), e);
                }
            }
        }

        @Override
        public void shutdown() {
            log.info("process shutdown du to finished for table: " + tableName);
        }
    }

    enum TargetDynamoDBType {
        Schema,
        Data,
        SchemaAndData;

        static boolean migrateSchema(String type) {
            // 只要不是Data，就迁移Schema
            return !TargetDynamoDBType.Data.name().equalsIgnoreCase(type);
        }

        static boolean migrateData(String type) {
            // 只要不是Schema，就迁移Data
            return !TargetDynamoDBType.Schema.name().equalsIgnoreCase(type);
        }
    }
}