// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringUtil implements ApplicationContextAware {

    private ApplicationContext applicationContext = null;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        if(this.applicationContext == null){
            this.applicationContext  = applicationContext;
        }
    }

    //获取 applicationContext
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
