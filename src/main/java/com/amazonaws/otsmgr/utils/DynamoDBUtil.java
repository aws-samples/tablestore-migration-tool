// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.utils;

import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.IndexType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyType;
import com.amazonaws.otsmgr.beans.MigrationColumn;
import com.amazonaws.otsmgr.beans.MigrationKey;
import com.amazonaws.otsmgr.beans.MigrationTable;
import com.amazonaws.otsmgr.conf.MigrationConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

import java.util.*;

@Component
public class DynamoDBUtil {

    private final Logger log = LoggerFactory.getLogger(DynamoDBUtil.class);

    private DynamoDbClient ddb = null;

    @Resource
    private MigrationConfig config;

    @PostConstruct
    public void init() {
        String region = config.getTargetRegion();
        ddb = DynamoDbClient.builder().region(Region.of(region)).build();
    }

    public String createTable(MigrationTable migrationTable) {
        List<AttributeDefinition> attrs = new ArrayList<>();
        Map<String, KeySchemaElement> keysMap = new LinkedHashMap();
        // for PK
        AttributeDefinition attr = AttributeDefinition.builder()
                .attributeName(migrationTable.getTableMeta().getPrimaryKeyList().get(0).getName())
                .attributeType(convertKeyType(migrationTable.getTableMeta().getPrimaryKeyList().get(0).getType()))
                .build();
        attrs.add(attr);

        KeySchemaElement key = KeySchemaElement.builder()
                .attributeName(migrationTable.getTableMeta().getPrimaryKeyList().get(0).getName())
                .keyType(KeyType.HASH)
                .build();
        keysMap.put(migrationTable.getTableMeta().getPrimaryKeyList().get(0).getName(), key);

        if(migrationTable.getTableMeta().getPrimaryKeyList().size() > 1) {
            // for Sort Key
            AttributeDefinition _attr = AttributeDefinition.builder()
                    .attributeName(migrationTable.getTableMeta().getPrimaryKeyList().get(1).getName())
                    .attributeType(convertKeyType(migrationTable.getTableMeta().getPrimaryKeyList().get(1).getType()))
                    .build();
            attrs.add(_attr);

            KeySchemaElement _key = KeySchemaElement.builder()
                    .attributeName(migrationTable.getTableMeta().getPrimaryKeyList().get(1).getName())
                    .keyType(KeyType.RANGE)
                    .build();
            keysMap.put(migrationTable.getTableMeta().getPrimaryKeyList().get(1).getName(), _key);
        }

        // for other attribute
        if(migrationTable.getTableMeta().getPrimaryKeyList().size() > 2) {
            for(int i = 2; i < migrationTable.getTableMeta().getPrimaryKeyList().size(); i++) {
                AttributeDefinition _attr = AttributeDefinition.builder()
                        .attributeName(migrationTable.getTableMeta().getPrimaryKeyList().get(i).getName())
                        .attributeType(convertKeyType(migrationTable.getTableMeta().getPrimaryKeyList().get(i).getType()))
                        .build();
                attrs.add(_attr);
            }
        }

        //determine billing mode
        int readCapacity = migrationTable.getReservedThroughput().getCapacityUnit().getReadCapacityUnit();
        int writeCapacity = migrationTable.getReservedThroughput().getCapacityUnit().getWriteCapacityUnit();
        boolean provision = readCapacity!=0 || writeCapacity!=0;

        // for Local Secondary Key and Global Secondary Key
        List<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<>();
        List<GlobalSecondaryIndex> globalSecondaryIndex = new ArrayList<>();

        // note 如果TableStore的主键数量大于2，会自动创建一个包含了所有TableStore主键的索引
        if(attrs.size() > 2) {
            // for projection
            List<String> nonKeyAttrs = new ArrayList<>();
            for (int i = 2; i < attrs.size(); i++) {
                nonKeyAttrs.add(attrs.get(i).attributeName());
            }
            Projection projection = Projection.builder().projectionType(ProjectionType.INCLUDE).nonKeyAttributes(nonKeyAttrs).build();
            LocalSecondaryIndex lindex = LocalSecondaryIndex.builder()
                    .indexName(migrationTable.getTableMeta().getTableName()+"_local_index").keySchema(new ArrayList<>(keysMap.values())).projection(projection).build();
            localSecondaryIndexes.add(lindex);
        }

        List<AttributeDefinition> ddb_table_attrs = new ArrayList<>(attrs.subList(0, 2));
        if(migrationTable.getIndexMetas() != null && migrationTable.getIndexMetas().size() > 0) {
            for (IndexMeta indexMeta : migrationTable.getIndexMetas()) {
                // index primary keys
                List<KeySchemaElement> index_keys = new ArrayList<>();
                index_keys.add(KeySchemaElement.builder()
                            .attributeName(indexMeta.getPrimaryKeyList().get(0))
                            .keyType(KeyType.HASH)
                            .build());

                if(indexMeta.getPrimaryKeyList().size() > 1) {
                    index_keys.add(KeySchemaElement.builder()
                            .attributeName(indexMeta.getPrimaryKeyList().get(1))
                            .keyType(KeyType.RANGE)
                            .build());
                    for(AttributeDefinition _attr : attrs) {
                        if(!ddb_table_attrs.contains(_attr) && _attr.attributeName().equalsIgnoreCase(indexMeta.getPrimaryKeyList().get(1))) {
                            ddb_table_attrs.add(_attr);
                        }
                    }
                }

                // for projection
                Projection projection = Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build();
                ArrayList<String> columns = new ArrayList<>();
                if(indexMeta.getPrimaryKeyList().size() > 2) {
                    for(int i = 2; i < indexMeta.getPrimaryKeyList().size(); i++) {
                        columns.add(indexMeta.getPrimaryKeyList().get(i));
                    }
                    if(indexMeta.getDefinedColumnsList()!=null && indexMeta.getDefinedColumnsList().size() > 0) {
                        columns.addAll(indexMeta.getDefinedColumnsList());
                        projection = Projection.builder().projectionType(ProjectionType.INCLUDE).nonKeyAttributes(columns).build();
                    }
                }

                // for build local indexes
                if(indexMeta.getIndexType().equals(IndexType.IT_LOCAL_INDEX)) {
                    LocalSecondaryIndex lindex = LocalSecondaryIndex.builder()
                            .indexName(indexMeta.getIndexName()).keySchema(index_keys).projection(projection).build();
                    localSecondaryIndexes.add(lindex);
                }
                // for build global indexes
                else if(indexMeta.getIndexType().equals(IndexType.IT_GLOBAL_INDEX)) {
                    if(provision) {
                        ProvisionedThroughput provisionedThroughput = ProvisionedThroughput.builder()
                                .readCapacityUnits((long)readCapacity).writeCapacityUnits((long) writeCapacity).build();
                        GlobalSecondaryIndex gindex = GlobalSecondaryIndex.builder()
                                .indexName(indexMeta.getIndexName()).provisionedThroughput(provisionedThroughput).keySchema(index_keys).projection(projection).build();
                        globalSecondaryIndex.add(gindex);
                    }
                    else {
                        GlobalSecondaryIndex gindex = GlobalSecondaryIndex.builder()
                                .indexName(indexMeta.getIndexName()).onDemandThroughput(OnDemandThroughput.builder()
                                        .maxReadRequestUnits(50L).maxWriteRequestUnits(50L).build()).keySchema(index_keys).projection(projection).build();
                        globalSecondaryIndex.add(gindex);
                    }
                }
            }
        }

        // provision mode
        CreateTableRequest request = null;
        if(provision) {
            request = CreateTableRequest.builder()
                    .attributeDefinitions(ddb_table_attrs)
                    .keySchema(keysMap.values())
                    .localSecondaryIndexes(localSecondaryIndexes.size()>0?localSecondaryIndexes:null)
                    .globalSecondaryIndexes(globalSecondaryIndex.size()>0?globalSecondaryIndex:null)
                    .billingMode("PROVISIONED")
                    .provisionedThroughput(ProvisionedThroughput.builder()
                            .readCapacityUnits((long) readCapacity)
                            .writeCapacityUnits((long) writeCapacity)
                            .build())
                    .tableName(migrationTable.getTableMeta().getTableName())
                    .build();
        }
        // ondemand mode
        else {
            request = CreateTableRequest.builder()
                .attributeDefinitions(ddb_table_attrs)
                .keySchema(keysMap.values())
                    .localSecondaryIndexes(localSecondaryIndexes.size()>0?localSecondaryIndexes:null)
                    .globalSecondaryIndexes(globalSecondaryIndex.size()>0?globalSecondaryIndex:null)
                .billingMode("PAY_PER_REQUEST")
                .onDemandThroughput(OnDemandThroughput.builder().maxReadRequestUnits(50L).maxWriteRequestUnits(50L).build())
                .tableName(migrationTable.getTableMeta().getTableName())
                .build();
        }

        String newTable;
        try {
            CreateTableResponse response = ddb.createTable(request);

            // Wait until the Amazon DynamoDB table is created.
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(migrationTable.getTableMeta().getTableName())
                    .build();
            DynamoDbWaiter dbWaiter = ddb.waiter();
            WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableExists(tableRequest);
            waiterResponse.matched().response().ifPresent(s -> log.info("Response received: {}", s));
            newTable = response.tableDescription().tableName();
            return newTable;

        } catch (DynamoDbException e) {
            log.error(e.getMessage());
        }
        return "";
    }

    public void deleteTable(String tableName) {
        DeleteTableRequest request = DeleteTableRequest.builder()
                .tableName(tableName)
                .build();
        try {
            ddb.deleteTable(request);

            // Wait until the Amazon DynamoDB table is created.
            DescribeTableRequest tableRequest = DescribeTableRequest.builder()
                    .tableName(tableName)
                    .build();
            DynamoDbWaiter dbWaiter = ddb.waiter();
            WaiterResponse<DescribeTableResponse> waiterResponse = dbWaiter.waitUntilTableNotExists(tableRequest);
            waiterResponse.matched().response().ifPresent(s -> log.info("Response received: {}", s));
        } catch (DynamoDbException e) {
            log.warn("Failed to delete table : " + e.getMessage());
        }
        log.info("Table deletion complete: " + tableName);
    }

    public void deleteTableItem(String tableName, List<MigrationKey> keys) {
        // Only need the primary key and sort key to delete.
        int index = keys.size()<2 ? keys.size() : 2;
        Map<String, AttributeValue> keyToDelete = buildKeyItems(keys.subList(0, index));

        DeleteItemRequest deleteReq = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(keyToDelete)
                .build();

        try {
            ddb.deleteItem(deleteReq);
        } catch (DynamoDbException e) {
            log.error("Error delete DynamoDB item: " + e.getMessage());
        }
    }

    public void insertTableItem(String tableName, List<MigrationKey> keys,
                                       List<MigrationColumn> columns) {
        Map<String, AttributeValue> itemValues = buildKeyItems(keys);

        for (MigrationColumn c : columns) {
            switch (c.getType()) {
                case STRING:
                    itemValues.put(c.getName(), AttributeValue.builder().s(c.getValue()).build());
                    break;

                case INTEGER:
                    itemValues.put(c.getName(), AttributeValue.builder().n(c.getValue()).build());
                    break;
            }
        }

        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(itemValues)
                .build();

        try {
            ddb.putItem(request);
            log.info(tableName +" was successfully inserted");

        } catch (ResourceNotFoundException e) {
            log.error("Error insert to DynamoDB: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
        } catch (DynamoDbException e) {
            log.error("Error insert to DynamoDB: " + e.getMessage());
        }
    }

    public void updateTableItem(String tableName, List<MigrationKey> keys,
                                       List<MigrationColumn> columns){

        Map<String,AttributeValue> itemKey = buildKeyItems(keys);

        HashMap<String,AttributeValueUpdate> values = new HashMap<>();
        // Update the column specified by name with updatedValue
        for (MigrationColumn c : columns) {
            switch (c.getType()) {
                case STRING:
                    values.put(c.getName(), AttributeValueUpdate.builder()
                            .value(AttributeValue.builder().s(c.getValue()).build())
                            .action(AttributeAction.PUT)
                            .build());
                    break;

                case INTEGER:
                    values.put(c.getName(), AttributeValueUpdate.builder()
                            .value(AttributeValue.builder().n(c.getValue()).build())
                            .action(AttributeAction.PUT)
                            .build());
                    break;
            }
        }

        UpdateItemRequest request = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(itemKey)
                .attributeUpdates(values)
                .build();

        try {
            ddb.updateItem(request);
        } catch (ResourceNotFoundException e) {
            log.error("Error update to DynamoDB: The Amazon DynamoDB table \"%s\" can't be found.\n", tableName);
        } catch (DynamoDbException e) {
            log.error("Error update to DynamoDB: " + e.getMessage());
        }
    }

    private Map<String,AttributeValue> buildKeyItems(List<MigrationKey> keys) {
        Map<String, AttributeValue> itemKeys = new HashMap<>();

        for (MigrationKey k : keys) {
            switch (k.getType()) {
                case STRING:
                    itemKeys.put(k.getName(), AttributeValue.builder().s(k.getValue()).build());
                    break;

                case INTEGER:
                    itemKeys.put(k.getName(), AttributeValue.builder().n(k.getValue()).build());
                    break;
            }
        }

        return itemKeys;
    }

    private static ScalarAttributeType convertKeyType(PrimaryKeyType type) {
        ScalarAttributeType dynamoDBType = ScalarAttributeType.S;
        switch (type) {
            case STRING:
                dynamoDBType = ScalarAttributeType.S;
                break;

            case INTEGER:
                dynamoDBType = ScalarAttributeType.N;
                break;

            case BINARY:
                dynamoDBType = ScalarAttributeType.B;
                break;
        }
        return dynamoDBType;
    }

    private static ScalarAttributeType convertIndexType(IndexType type) {
        ScalarAttributeType dynamoDBType = ScalarAttributeType.S;
        switch (type) {
            case IT_LOCAL_INDEX:
                dynamoDBType = ScalarAttributeType.S;
                break;

            case IT_GLOBAL_INDEX:
                dynamoDBType = ScalarAttributeType.N;
                break;
        }
        return dynamoDBType;
    }
}
