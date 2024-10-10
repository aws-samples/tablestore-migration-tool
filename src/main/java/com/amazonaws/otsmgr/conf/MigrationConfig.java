// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.conf;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "ots.migration.config")
public class MigrationConfig {

    private String sourceEndPoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String instanceName;
    private List<String> tableNames;
    private String ddbEndpoint;
    private String targetRegion;
    private String s3BuckeName;
    private String migrationTarget;
    private String targetDynamodbType;
    private String migrationType;
    private boolean restart;

    private Map<String, String[]> tablePKs;
    private Map<String, String[]> tableColumns;
    private Map<String, String[]> allTableColumns;

    @PostConstruct
    private void init() {
        allTableColumns = new HashMap<>();
        for (String tableName : tableNames) {
            String[] allTableColumnsArr = ArrayUtils.addAll(tablePKs.get(tableName), tableColumns.get(tableName));
            allTableColumns.put(tableName, allTableColumnsArr);
        }
    }

    public void setSourceEndPoint(String sourceEndPoint) {
        this.sourceEndPoint = sourceEndPoint;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public void setDdbEndpoint(String ddbEndpoint) {
        this.ddbEndpoint = ddbEndpoint;
    }
    public void setTargetRegion(String targetRegion) {
        this.targetRegion = targetRegion;
    }
    public void setS3BuckeName(String s3BuckeName) {
        this.s3BuckeName = s3BuckeName;
    }

    public void setMigrationTarget(String migrationTarget) {
        this.migrationTarget = migrationTarget;
    }

    public void setTargetDynamodbType(String targetDynamodbType) {
        this.targetDynamodbType = targetDynamodbType;
    }

    public void setMigrationType(String migrationType) {
        this.migrationType = migrationType;
    }

    public void setRestart(boolean restart) {
        this.restart = restart;
    }

    public void setTablePKs(Map<String, String[]> tablePKs) {
        this.tablePKs = tablePKs;
    }

    public void setTableColumns(Map<String, String[]> tableColumns) {
        this.tableColumns = tableColumns;
    }

    public String getSourceEndPoint() {
        return sourceEndPoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public String getDdbEndpoint() {
        return ddbEndpoint;
    }

    public String getTargetRegion() {
        return targetRegion;
    }

    public String getS3BuckeName() {
        return s3BuckeName;
    }

    public Map<String, String[]> getTablePKs() {
        return tablePKs;
    }

    public Map<String, String[]> getTableColumns() {
        return tableColumns;
    }

    public Map<String, String[]>  getAllTableColumns() {
        return allTableColumns;
    }

    public String getMigrationTarget() {
        return migrationTarget;
    }

    public String getTargetDynamodbType() {
        return targetDynamodbType;
    }

    public String getMigrationType() {
        return migrationType;
    }

    public boolean isRestart() {
        return restart;
    }
}
