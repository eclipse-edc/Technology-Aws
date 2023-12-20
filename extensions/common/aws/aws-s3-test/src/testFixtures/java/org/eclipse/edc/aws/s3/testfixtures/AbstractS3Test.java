/*
 *  Copyright (c) 2020 - 2022 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *       NTT DATA - added endpoint override
 *
 */

package org.eclipse.edc.aws.s3.testfixtures;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import software.amazon.awssdk.regions.Region;

import java.io.IOException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.util.configuration.ConfigurationFunctions.propOrEnv;

/**
 * Base class for tests that need an S3 bucket created and deleted on every test run.
 */
public abstract class AbstractS3Test {

    protected static final String REGION = propOrEnv("it.aws.region", Region.US_EAST_1.id());
    // Adding REGION to bucket prevents errors of
    //      "A conflicting conditional operation is currently in progress against this resource."
    // when bucket is rapidly added/deleted and consistency propagation causes this error.
    // (Should not be necessary if REGION remains static, but added to prevent future frustration.)
    // [see http://stackoverflow.com/questions/13898057/aws-error-message-a-conflicting-conditional-operation-is-currently-in-progress]
    protected static final String SOURCE_MINIO_ENDPOINT = "http://localhost:9000";

    protected static final String DESTINATION_MINIO_ENDPOINT = "http://localhost:9002";

    protected String bucketName = "test-bucket-" + UUID.randomUUID() + "-" + REGION;

    protected S3TestClient sourceClient = S3TestClient.create(SOURCE_MINIO_ENDPOINT, REGION);

    protected S3TestClient destinationClient = S3TestClient.create(DESTINATION_MINIO_ENDPOINT, REGION);

    protected static final String ASSET_PREFIX = "folderName/";

    protected static final String ASSET_FILE = "text-document.txt";

    @BeforeAll
    void prepareAll() {
        await().atLeast(Duration.ofSeconds(2))
            .atMost(Duration.ofSeconds(15))
            .with()
            .pollInterval(Duration.ofSeconds(2))
            .ignoreException(IOException.class) // thrown by pingMinio
            .ignoreException(ConnectException.class)
            .until(this::isBackendAvailable);
    }

    private boolean isBackendAvailable() throws IOException {
        if (sourceClient.isMinio() && destinationClient.isMinio()) {
            return sourceClient.isAvailable() && destinationClient.isAvailable();
        } else {
            return true;
        }
    }

    @BeforeEach
    public void setupClients() {
        sourceClient.createBucket(bucketName);
    }

    @AfterEach
    void cleanup() {
        sourceClient.deleteBucket(bucketName);
    }
}
