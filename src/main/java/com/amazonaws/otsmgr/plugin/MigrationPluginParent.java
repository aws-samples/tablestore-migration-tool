// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.plugin;

import com.amazonaws.otsmgr.beans.MigrationTable;
import com.amazonaws.otsmgr.utils.OTSUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public abstract class MigrationPluginParent implements MigrationPluginInterface {
    private static final Logger log = LoggerFactory.getLogger(MigrationPluginParent.class);

    @Autowired
    private OTSUtil otsUtil;

    public List<MigrationTable> sourceTables() {
        log.info("Get from source TableStore.");
        List<MigrationTable> migrationTables = otsUtil.getAllTableSchema();
        log.info("Finished Get from source TableStore.");
        return migrationTables;
    }
}
