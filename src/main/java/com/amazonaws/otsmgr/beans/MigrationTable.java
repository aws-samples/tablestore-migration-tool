// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.beans;

import com.alicloud.openservices.tablestore.model.*;

import java.util.List;

public class MigrationTable {
    private TableMeta tableMeta;
    private List<IndexMeta> indexMetas;
    private TableOptions tableOptions;
    private ReservedThroughputDetails reservedThroughput;

    public TableMeta getTableMeta() {
        return tableMeta;
    }

    public void setTableMeta(TableMeta tableMeta) {
        this.tableMeta = tableMeta;
    }

    public List<IndexMeta> getIndexMetas() {
        return indexMetas;
    }

    public void setIndexMetas(List<IndexMeta> indexMetas) {
        this.indexMetas = indexMetas;
    }

    public TableOptions getTableOptions() {
        return tableOptions;
    }

    public void setTableOptions(TableOptions tableOptions) {
        this.tableOptions = tableOptions;
    }

    public ReservedThroughputDetails getReservedThroughput() {
        return reservedThroughput;
    }

    public void setReservedThroughput(ReservedThroughputDetails reservedThroughput) {
        this.reservedThroughput = reservedThroughput;
    }
}
