// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.beans;

import com.alicloud.openservices.tablestore.model.ColumnType;

public class MigrationColumn {
    private String name;
    private ColumnType type;
    private String value;

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public String getValue() {
        return value;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof MigrationColumn)) {
            return false;
        }

        MigrationColumn col = (MigrationColumn) o;
        return this.name.equalsIgnoreCase(col.getName());
    }
}
