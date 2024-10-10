// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr;

import com.amazonaws.otsmgr.conf.MigrationConfig;
import com.amazonaws.otsmgr.plugin.MigrationPluginInterface;
import com.amazonaws.otsmgr.plugin.MigrationTargetEnum;
import com.amazonaws.otsmgr.service.MigrationService;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class OtsMgrApplication {

    private static final Logger log = LoggerFactory.getLogger(OtsMgrApplication.class);

    @Resource
    private MigrationConfig config;

    public static void main(String[] args) {
        SpringApplication.run(OtsMgrApplication.class, args);
    }

    @Bean
    ApplicationRunner runner(MigrationService migrationService) {
        return args -> {
            if (args.containsOption("restart")) {
                config.setRestart(true);
            }

            MigrationTargetEnum target = MigrationTargetEnum.valueOfIgnoreCase(config.getMigrationTarget());
            MigrationPluginInterface migrationPlugin = migrationService.chooseMigrationTarget(target);
            migrationPlugin.migrate(config);
        };
    }
}