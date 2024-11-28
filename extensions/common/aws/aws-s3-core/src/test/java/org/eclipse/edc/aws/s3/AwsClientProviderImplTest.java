/*
 *  Copyright (c) 2024 Cofinity-X
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Cofinity-X - initial API and implementation
 *
 */

package org.eclipse.edc.aws.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

class AwsClientProviderImplTest {

    private AwsClientProvider clientProvider;
    
    @BeforeEach
    void setUp() {
        var config = AwsClientProviderConfiguration.Builder.newInstance()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        clientProvider = new AwsClientProviderImpl(config);
    }
    
    @Test
    void iamAsyncClient_noEndpointOverride_shouldReturnClient() {
        var clientRequest = S3ClientRequest.from("region", null);
        
        var client = clientProvider.iamAsyncClient(clientRequest);
        
        assertThat(client).isNotNull();
        assertThat(client.serviceClientConfiguration().endpointOverride()).isEmpty();
    }
    
    @Test
    void iamAsyncClient_requestMultipleTimesNoEndpointOverride_shouldReturnSameClient() {
        var clientRequest = S3ClientRequest.from("region", null);
        
        var client1 = clientProvider.iamAsyncClient(clientRequest);
        var client2 = clientProvider.iamAsyncClient(clientRequest);
        
        assertThat(client1).isSameAs(client2);
    }
    
    @Test
    void iamAsyncClient_withEndpointOverride_shouldReturnClient() {
        var endpointOverride = "https://endpointOverride";
        var clientRequest = S3ClientRequest.from("region", endpointOverride);
    
        var client = clientProvider.iamAsyncClient(clientRequest);
    
        assertThat(client).isNotNull();
        assertThat(client.serviceClientConfiguration().endpointOverride()).contains(URI.create(endpointOverride));
    }
    
    @Test
    void iamAsyncClient_requestMultipleTimesWithEndpointOverride_shouldReturnSameClient() {
        var clientRequest = S3ClientRequest.from("region", "https://endpointOverride");
        
        var client1 = clientProvider.iamAsyncClient(clientRequest);
        var client2 = clientProvider.iamAsyncClient(clientRequest);
        
        assertThat(client1).isSameAs(client2);
    }
}
