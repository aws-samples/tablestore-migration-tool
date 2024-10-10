// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.beans;

import com.alicloud.openservices.tablestore.model.PrimaryKeyType;

public class MigrationKey {
    private String name;
    private PrimaryKeyType type;
    private String value;

    public String getName() {
        return name;
    }

    public PrimaryKeyType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(PrimaryKeyType type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
