/*
 *  Copyright (c) 2020, 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.S3BucketSchema;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.StatusChecker;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.spi.EdcException;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.List;
import java.util.concurrent.CompletionException;

import static java.lang.String.format;

public class S3StatusChecker implements StatusChecker {
    private final AwsClientProvider clientProvider;
    private final RetryPolicy<Object> retryPolicy;

    public S3StatusChecker(AwsClientProvider clientProvider, RetryPolicy<Object> retryPolicy) {
        this.clientProvider = clientProvider;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public boolean isComplete(TransferProcess transferProcess, List<ProvisionedResource> resources) {
        if (resources.isEmpty()) {
            var destination = transferProcess.getDataRequest().getDataDestination();
            var bucketName = destination.getStringProperty(S3BucketSchema.BUCKET_NAME);
            var region = destination.getStringProperty(S3BucketSchema.REGION);
            return checkBucket(bucketName, region);
        } else {
            for (var resource : resources) {
                if (resource instanceof S3BucketProvisionedResource) {
                    var provisionedResource = (S3BucketProvisionedResource) resource;
                    try {
                        var bucketName = provisionedResource.getBucketName();
                        var region = provisionedResource.getRegion();
                        return checkBucket(bucketName, region);
                    } catch (CompletionException cpe) {
                        if (cpe.getCause() instanceof NoSuchBucketException) {
                            return false;
                        }
                        throw cpe;
                    }

                }
            }

        }

        // otherwise, we have an implementation error
        throw new EdcException(format("No bucket resource was associated with the transfer process: %s - cannot determine completion.", transferProcess.getId()));
    }

    private boolean checkBucket(String bucketName, String region) {
        try {
            var s3client = clientProvider.s3AsyncClient(region);

            var rq = ListObjectsRequest.builder().bucket(bucketName).build();
            var response = Failsafe.with(retryPolicy)
                    .getStageAsync(() -> s3client.listObjects(rq))
                    .join();
            return response.contents().stream().anyMatch(s3object -> s3object.key().endsWith(".complete"));
        } catch (CompletionException ex) {
            if (ex.getCause() instanceof S3Exception) {
                return false;
            } else {
                throw ex;
            }
        }
    }

}
