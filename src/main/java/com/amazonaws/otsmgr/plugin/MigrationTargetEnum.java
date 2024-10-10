// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.plugin;

public enum MigrationTargetEnum {
    DynamoDB,
    S3,
    MongoDB;


    public static MigrationTargetEnum valueOfIgnoreCase(String name) {
        for (MigrationTargetEnum value : values()) {
            if (value.name().equalsIgnoreCase(name)) {
                return value;
            }
        }
        throw new IllegalArgumentException("No enum constant " + name);
    }
}