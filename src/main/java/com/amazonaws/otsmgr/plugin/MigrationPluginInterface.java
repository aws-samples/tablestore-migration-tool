// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.plugin;

import com.amazonaws.otsmgr.conf.MigrationConfig;
import org.springframework.plugin.core.Plugin;

public interface MigrationPluginInterface extends Plugin<MigrationTargetEnum> {
//    void cleanTargetTables(MigrationConfig migrationConfigs);

    void migrate(MigrationConfig migrationConfig);
}