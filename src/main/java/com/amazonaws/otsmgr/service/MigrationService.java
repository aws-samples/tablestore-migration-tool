
// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.service;

import java.util.Optional;

import com.amazonaws.otsmgr.plugin.MigrationPluginInterface;
import com.amazonaws.otsmgr.plugin.MigrationTargetEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.plugin.core.PluginRegistry;
import org.springframework.plugin.core.config.EnablePluginRegistries;
import org.springframework.stereotype.Service;

@Service
@EnablePluginRegistries(MigrationPluginInterface.class)
public class MigrationService {
    private final PluginRegistry<MigrationPluginInterface, MigrationTargetEnum> plugins;

    @Autowired
    public MigrationService(PluginRegistry<MigrationPluginInterface, MigrationTargetEnum> _plugins) {
        this.plugins = _plugins;
    }

    public MigrationPluginInterface chooseMigrationTarget(MigrationTargetEnum migrationTarget) {
        Optional<MigrationPluginInterface> pluginFor = plugins.getPluginFor(migrationTarget);

        if (pluginFor.isPresent()) {
            return pluginFor.get();
        }

        throw new UnsupportedOperationException("No such migration target.");
    }
}