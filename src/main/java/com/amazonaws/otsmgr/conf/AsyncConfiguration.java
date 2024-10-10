// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.conf;

import jakarta.annotation.Resource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
public class AsyncConfiguration {

    @Resource
    private MigrationConfig config;

    @Bean(name = "taskExecutor")
    public ThreadPoolTaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(100);
        executor.setKeepAliveSeconds(60);
        executor.setAllowCoreThreadTimeOut(true);
        executor.setAwaitTerminationSeconds(60);
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setThreadNamePrefix("otsmgr-async-");
        executor.initialize();
        return executor;
    }
}