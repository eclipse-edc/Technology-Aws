/*
 *  Copyright (c) 2022 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - Initial implementation
 *       ZF Friedrichshafen AG - Initial implementation
 *
 */

package org.eclipse.edc.aws.s3;

import org.eclipse.edc.runtime.metamodel.annotation.ExtensionPoint;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsAsyncClient;

/**
 * Provide various AWS client shapes
 * <br>
 * Caching by region:
 * - S3Client
 * - S3AsyncClient
 * - StsAsyncClient
 * <br>
 * Single instance for the aws-global region:
 * - IamAsyncClient
 * <br>
 * Instantiated on-fly given a SecretToken:
 * - S3Client
 */
@ExtensionPoint
public interface AwsClientProvider {
    /**
     * Returns the client for the specified s3ClientRequest
     */
    S3Client s3Client(S3ClientRequest s3ClientRequest);

    /**
     * @deprecated Use {@link AwsClientProvider#s3AsyncClient(S3ClientRequest)} instead!
     */
    @Deprecated
    default S3AsyncClient s3AsyncClient(String region){
        return s3AsyncClient(S3ClientRequest.from(region, null));
    }
    /**
     * Returns the s3 async client for the specified region
     */
    S3AsyncClient s3AsyncClient(S3ClientRequest s3ClientRequest);

    /**
     * @deprecated Use {@link AwsClientProvider#iamAsyncClient(S3ClientRequest)} instead!
     */
    @Deprecated
    default IamAsyncClient iamAsyncClient(){
        return iamAsyncClient(S3ClientRequest.from(Region.AWS_GLOBAL.id(), null));
    }
    /**
     * Returns the iam async client for the global region
     */
    IamAsyncClient iamAsyncClient(S3ClientRequest s3ClientRequest);

    /**
     * @deprecated Use {@link AwsClientProvider#stsAsyncClient(S3ClientRequest)} instead!
     */
    @Deprecated
    default StsAsyncClient stsAsyncClient(String region){
        return stsAsyncClient(S3ClientRequest.from(region, null));
    }
    /**
     * Returns the sts async client for the specified region
     */
    StsAsyncClient stsAsyncClient(S3ClientRequest s3ClientRequest);

    /**
     * Releases resources used by the provider.
     */
    void shutdown();
}
