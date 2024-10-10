// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.utils;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.tunnel.*;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import com.amazonaws.otsmgr.conf.MigrationConfig;
import com.amazonaws.otsmgr.beans.MigrationTable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class OTSUtil {
    private final Logger log = LoggerFactory.getLogger(OTSUtil.class);

    @Resource
    private MigrationConfig migrationConfig;

    private SyncClient client;

    private TunnelClient tunnelClient;

    @PostConstruct
    private void init() {
        client = new SyncClient(migrationConfig.getSourceEndPoint(), migrationConfig.getAccessKeyId(), migrationConfig.getAccessKeySecret(), migrationConfig.getInstanceName());
        tunnelClient = new TunnelClient(migrationConfig.getSourceEndPoint(), migrationConfig.getAccessKeyId(), migrationConfig.getAccessKeySecret(), migrationConfig.getInstanceName());
    }

    public List<MigrationTable> getAllTableSchema() {
        List<MigrationTable> tableSchemas = new ArrayList<>();
        ListTableResponse listTableResponse = client.listTable();
        for (String tableName : listTableResponse.getTableNames()) {
            MigrationTable migrationTable = new MigrationTable();
            tableSchemas.add(migrationTable);

            DescribeTableRequest request = new DescribeTableRequest(tableName);
            DescribeTableResponse response = client.describeTable(request);
            TableMeta tableMeta = response.getTableMeta();
            migrationTable.setTableMeta(tableMeta);
            log.info("Table name：{}", tableMeta.getTableName());
            log.info("Table primary key：");
            for (PrimaryKeySchema primaryKeySchema : tableMeta.getPrimaryKeyList()) {
                log.info("\t{}", primaryKeySchema);
            }

            List<IndexMeta> indexMetas = response.getIndexMeta();
            migrationTable.setIndexMetas(indexMetas);
            log.info("Table indexes：");
            for (IndexMeta indexMeta : indexMetas) {
                log.info("\t{}", indexMeta);
            }

            TableOptions tableOptions = response.getTableOptions();
            migrationTable.setTableOptions(tableOptions);
            log.info("Table TTL: {}", tableOptions.getTimeToLive());
            log.info("Table MaxVersions: {}", tableOptions.getMaxVersions());
            //只能查看加密表的加密配置信息。非加密表无此配置项。
            //System.out.println("表的加密配置：" + response.getSseDetails());
            ReservedThroughputDetails reservedThroughputDetails = response.getReservedThroughputDetails();
            migrationTable.setReservedThroughput(reservedThroughputDetails);
            log.info("Table reserved read throughput：{}", reservedThroughputDetails.getCapacityUnit().getReadCapacityUnit());
            log.info("Table reserved write throughput：{}", reservedThroughputDetails.getCapacityUnit().getWriteCapacityUnit());
        }
        return tableSchemas;
    }

    public String getTunel(String tableName) {
        String tunnelId = null;
        String tunnelName = tableName + "_migration2aws_tunnel4" + migrationConfig.getMigrationTarget();
        try {
            DescribeTunnelRequest drequest = new DescribeTunnelRequest(tableName, tunnelName);
            DescribeTunnelResponse dresp = tunnelClient.describeTunnel(drequest);
            tunnelId = dresp.getTunnelInfo().getTunnelId();
        } catch (Exception be) {
            CreateTunnelRequest crequest = new CreateTunnelRequest(tableName, tunnelName, TunnelType.valueOf(migrationConfig.getMigrationType()));
            CreateTunnelResponse cresp = tunnelClient.createTunnel(crequest);
            //tunnelId用于后续TunnelWorker的初始化，该值也可以通过ListTunnel或者DescribeTunnel获取。
            tunnelId = cresp.getTunnelId();
        }
        log.info("Tunnel found, Id: " + tunnelId);
        return tunnelId;
    }

    public void deleteTunel(String tableName) {
        String tunnelName = tableName + "_migration2aws_tunnel4" + migrationConfig.getMigrationTarget();
        try {
            DeleteTunnelRequest drequest = new DeleteTunnelRequest(tableName, tunnelName);
            DeleteTunnelResponse dresp = tunnelClient.deleteTunnel(drequest);
            log.info("Tunnel has been deleted: " + dresp.toString());
        } catch (Exception be) {
            log.warn("Tunnel deletion failed due to not found: " + tunnelName);
        }
    }

    public Runnable getRunner(IChannelProcessor processor, String migrationTable) {
        Runnable runnable = () -> {
            //TunnelWorkerConfig默认会启动读数据和处理数据的线程池。
            String tunnelId = getTunel(migrationTable);
            //如果使用的是单台机器，当需要启动多个TunnelWorker时，建议共用一个TunnelWorkerConfig。TunnelWorkerConfig中包括更多的高级参数。
            TunnelWorkerConfig workerConfig = new TunnelWorkerConfig(processor);
            //配置TunnelWorker，并启动自动化的数据处理任务。
            TunnelWorker worker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);
            try {
                worker.connectAndWorking();
            } catch (Exception e) {
                log.error("Start OTS tunnel failed.", e);
                worker.shutdown();
            }
        };
        return runnable;
    }
}