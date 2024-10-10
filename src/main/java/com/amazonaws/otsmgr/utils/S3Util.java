// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
package com.amazonaws.otsmgr.utils;

import com.amazonaws.otsmgr.conf.MigrationConfig;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class S3Util {

    private static final Logger log = LoggerFactory.getLogger(S3Util.class);

    private S3Client s3;
    private String BUCKET_NAME;
    private String REGION;
    private Map<String, String> METADATA = new HashMap<>();

    @Resource
    private MigrationConfig config;

    @PostConstruct
    public void initClient() {
        BUCKET_NAME =  config.getS3BuckeName();
        REGION = config.getTargetRegion();
        s3 = S3Client.builder()
                .region(Region.of(REGION))
                .build();
//        S3 = S3AsyncClient.crtBuilder()
//                .region(Region.of(REGION))
//                .targetThroughputInGbps(20.0)
//                .minimumPartSizeInBytes(8 * 1025 * 1024L)
//                .build();
        METADATA.put("Content-Type", "test/csv");
    }

    public String getBucketName() {
        return BUCKET_NAME;
    }

    public void uploadItem(String prefix, String key, ByteArrayOutputStream stream) {
        try {
            log.debug("Start to upload to S3.");
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key("otsmigration/" + prefix + "/" + key + ".csv")
                    .metadata(METADATA)
                    .build();
//            S3.putObject(putObjectRequest, RequestBody.fromInputStream(new ByteArrayInputStream(stream.toByteArray()), stream.toByteArray().length));
            s3.putObject(putObjectRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(stream.toByteArray())));
            log.debug("Finished to upload to S3.");

            log.info(key +" was successfully inserted");
        } catch (S3Exception e) {
              log.error("Error upload to S3: " + e.getMessage());
        }
    }

    public void deleteTable(String prefix) {
        try {
            log.info("Start to delete table files in S3: " + prefix);
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(BUCKET_NAME)
                    .prefix("otsmigration/" + prefix)
                    .build();
            ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);
            List<S3Object> objects = listObjectsResponse.contents();

            for (S3Object object : objects) {
                DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(object.key())
                        .build();
                s3.deleteObject(deleteRequest);
                log.info("Deleted object: " + object.key());
            }
            log.info("Finished to delete table files from S3: " + prefix);
        } catch (S3Exception e) {
            log.error("Error delete from S3: " + e.getMessage());
        }
    }
}
