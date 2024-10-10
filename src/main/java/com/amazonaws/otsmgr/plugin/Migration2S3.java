// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.plugin;

import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;
import com.amazonaws.otsmgr.beans.MigrationColumn;
import com.amazonaws.otsmgr.conf.MigrationConfig;
import com.amazonaws.otsmgr.utils.OTSUtil;
import com.amazonaws.otsmgr.utils.S3Util;
import com.opencsv.CSVWriter;
import jakarta.annotation.Resource;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class Migration2S3 extends MigrationPluginParent {
    private static final Logger log = LoggerFactory.getLogger(Migration2S3.class);

    @Resource
    private S3Util s3Util;

    @Resource
    private OTSUtil otsUtil;

    @Resource
    ThreadPoolTaskExecutor threadPoolTaskExecutor;

    @Override
    public void migrate(MigrationConfig config) {
        log.info("Start migrating TableStore to S3.");
        clean(config);
        migrateData(config);
        log.info("Finish migrating TableStore to S3.");
    }

    public void clean(MigrationConfig config) {
        if(config.isRestart()) {
            for (String migrationTable : config.getTableNames()) {
                otsUtil.deleteTunel(migrationTable);
                s3Util.deleteTable(migrationTable);
            }
        }
    }

    @Override
    public boolean supports(MigrationTargetEnum migrationTarget) {
        return migrationTarget == MigrationTargetEnum.S3;
    }

    private void migrateData(MigrationConfig config) {
        for (String tableName : config.getTableNames()) {
            log.info("Started migrating TableStore data to S3, " + tableName);
            Runnable runnable = otsUtil.getRunner(new S3Processor(tableName, config), tableName);
            threadPoolTaskExecutor.execute(runnable);
            log.info("Finished migrating TableStore data to S3, " + tableName);
        }
    }

    //用户自定义数据消费Callback，即实现IChannelProcessor接口（process和shutdown）。
    class S3Processor implements IChannelProcessor {
        private String tableName = null;
        private MigrationConfig config = null;

        public S3Processor(String tableName, MigrationConfig config) {
            this.tableName = tableName;
            this.config = config;
        }

        @Override
        public void process(ProcessRecordsInput input) {
            //ProcessRecordsInput中包含有拉取到的数据。
            log.info("S3 record processor, would migrate to S3: " + tableName);
            //NextToken用于Tunnel Client的翻页。
            log.info(String.format("Process %d records, NextToken: %s", input.getRecords().size(), input.getNextToken()));

            if (!CollectionUtils.isEmpty(input.getRecords())) {
                log.info("Start to deal with ProcessRecordsInput.");
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
                try (CSVWriter writer = buildCSVWriter(streamWriter)) {
                    List<String[]> lines = input.getRecords().stream()
                            .map(r -> {
                                String[] pk_values = Arrays.stream(r.getPrimaryKey().getPrimaryKeyColumns()).map(col -> {
                                    String _col = col.getValue().toString();
                                    return _col;
                                }).toArray(String[]::new);

                                List<MigrationColumn> mcols = r.getColumns().stream().map(col -> {
                                    MigrationColumn mcol = new MigrationColumn();
                                    mcol.setName(col.getColumn().getName().toString());
                                    mcol.setValue(col.getColumn().getValue().toString());
                                    return mcol;
                                }).collect(Collectors.toList());

                                String[] col_values = Arrays.stream(config.getTableColumns().get(tableName)).map(col -> {
                                    MigrationColumn _mcol = new MigrationColumn();
                                    _mcol.setName(col);
                                    int i = mcols.indexOf(_mcol);
                                    return i < 0 ? "" : mcols.get(i).getValue();
                                }).toArray(String[]::new);

                                return (String[]) ArrayUtils.addAll(pk_values, col_values);
                            })
                            .collect(Collectors.toList());
                    log.info("Finished to deal with ProcessRecordsInput.");
                    log.info("Start to write to ByteArrayOutputStream.");
                    lines.add(0, config.getAllTableColumns().get(tableName));
                    writer.writeAll(lines);
                    writer.flush();
                    log.info("Finished to write to ByteArrayOutputStream.");

                    s3Util.uploadItem(tableName, input.getTraceId(), stream);
                } catch (IOException e) {
                    log.error("Error upload to S3: " + e.getMessage());
                }
            }
        }

        @Override
        public void shutdown() {
            log.info("process shutdown du to finished for table: " + tableName);
        }

        private CSVWriter buildCSVWriter(OutputStreamWriter streamWriter) {
            return new CSVWriter(streamWriter, ',', Character.MIN_VALUE, '"', System.lineSeparator());
        }
    }
}