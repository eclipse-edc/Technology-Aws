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
 *       Cofinity-X - fix iamAsyncClient without endpointOverride
 *
 */

package org.eclipse.edc.aws.s3;

import org.eclipse.edc.spi.EdcException;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamAsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3BaseClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.awssdk.utils.ThreadFactoryBuilder;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR;

public class AwsClientProviderImpl implements AwsClientProvider {
    
    private static final String NO_ENDPOINT_OVERRIDE = "default";

    private final AwsCredentialsProvider credentialsProvider;
    private final AwsClientProviderConfiguration configuration;
    private final Executor executor;
    private final Map<String, S3Client> s3Clients = new ConcurrentHashMap<>();
    private final Map<String, S3AsyncClient> s3AsyncClients = new ConcurrentHashMap<>();
    private final Map<String, StsAsyncClient> stsAsyncClients = new ConcurrentHashMap<>();
    private final Map<String, IamAsyncClient> iamAsyncClients = new ConcurrentHashMap<>();

    public AwsClientProviderImpl(AwsClientProviderConfiguration configuration) {
        this.credentialsProvider = configuration.getCredentialsProvider();
        this.configuration = configuration;
        this.executor = Executors.newFixedThreadPool(configuration.getThreadPoolSize(), new ThreadFactoryBuilder().threadNamePrefix("aws-client").build());
    }

    @Override
    public S3Client s3Client(S3ClientRequest s3ClientRequest) {
        return createS3Client(s3ClientRequest);
    }

    @Override
    public S3AsyncClient s3AsyncClient(S3ClientRequest clientRequest) {
        return createS3AsyncClient(clientRequest);
    }

    @Override
    public IamAsyncClient iamAsyncClient(S3ClientRequest clientRequest) {
        var key = clientRequest.endpointOverride() != null ? clientRequest.endpointOverride() : NO_ENDPOINT_OVERRIDE;
        return iamAsyncClients.computeIfAbsent(key, s -> createIamAsyncClient(clientRequest.endpointOverride()));
    }

    @Override
    public StsAsyncClient stsAsyncClient(S3ClientRequest clientRequest) {
        var key = clientRequest.region() + "/" + clientRequest.endpointOverride();
        return stsAsyncClients.computeIfAbsent(key, s -> createStsClient(clientRequest.region(), clientRequest.endpointOverride()));
    }

    @Override
    public void shutdown() {
        iamAsyncClients.values().forEach(SdkAutoCloseable::close);
        s3AsyncClients.values().forEach(SdkAutoCloseable::close);
        stsAsyncClients.values().forEach(SdkAutoCloseable::close);
    }

    private S3Client createS3Client(S3ClientRequest s3ClientRequest) {

        var token = s3ClientRequest.secretToken();
        var region = s3ClientRequest.region();
        var endpointOverride = s3ClientRequest.endpointOverride();

        if (token != null) {
            if (token instanceof AwsTemporarySecretToken temporary) {
                var credentials = AwsSessionCredentials.create(temporary.accessKeyId(), temporary.secretAccessKey(),
                        temporary.sessionToken());
                return createS3Client(StaticCredentialsProvider.create(credentials), region, endpointOverride);
            }
            if (token instanceof AwsSecretToken secretToken) {
                var credentials = AwsBasicCredentials.create(secretToken.getAccessKeyId(), secretToken.getSecretAccessKey());
                return createS3Client(StaticCredentialsProvider.create(credentials), region, endpointOverride);
            }
            throw new EdcException(String.format("SecretToken %s is not supported", token.getClass()));
        } else {
            return s3Clients.computeIfAbsent(region, s3Client -> createS3Client(credentialsProvider, region, endpointOverride));
        }
    }

    private S3Client createS3Client(AwsCredentialsProvider credentialsProvider, String region, String endpointOverride) {
        var builder = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region));

        handleBaseEndpointOverride(builder, endpointOverride);

        return builder.build();
    }
    
    private S3AsyncClient createS3AsyncClient(S3ClientRequest s3ClientRequest) {
        var token = s3ClientRequest.secretToken();
        var region = s3ClientRequest.region();
        var endpointOverride = s3ClientRequest.endpointOverride();
    
        if (token != null) {
            if (token instanceof AwsTemporarySecretToken temporary) {
                var credentials = AwsSessionCredentials.create(temporary.accessKeyId(), temporary.secretAccessKey(),
                        temporary.sessionToken());
                return createS3AsyncClient(StaticCredentialsProvider.create(credentials), region, endpointOverride);
            }
            if (token instanceof AwsSecretToken secretToken) {
                var credentials = AwsBasicCredentials.create(secretToken.getAccessKeyId(), secretToken.getSecretAccessKey());
                return createS3AsyncClient(StaticCredentialsProvider.create(credentials), region, endpointOverride);
            }
            throw new EdcException(String.format("SecretToken %s is not supported", token.getClass()));
        } else {
            var key = s3ClientRequest.region() + "/" + s3ClientRequest.endpointOverride();
            return s3AsyncClients.computeIfAbsent(key, s3Client -> createS3AsyncClient(credentialsProvider, region, endpointOverride));
        }
    }

    private S3AsyncClient createS3AsyncClient(AwsCredentialsProvider credentialsProvider, String region, String endpointOverride) {
        var builder = S3AsyncClient.builder()
                .asyncConfiguration(b -> b.advancedOption(FUTURE_COMPLETION_EXECUTOR, executor))
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region));

        handleBaseEndpointOverride(builder, endpointOverride);

        return builder.build();
    }

    private StsAsyncClient createStsClient(String region, String endpointOverride) {
        var builder = StsAsyncClient.builder()
                .asyncConfiguration(b -> b.advancedOption(FUTURE_COMPLETION_EXECUTOR, executor))
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region));

        handleEndpointOverride(builder, endpointOverride);

        return builder.build();
    }

    private IamAsyncClient createIamAsyncClient(String endpointOverride) {
        var builder = IamAsyncClient.builder()
                .asyncConfiguration(b -> b.advancedOption(FUTURE_COMPLETION_EXECUTOR, executor))
                .credentialsProvider(credentialsProvider)
                .region(Region.AWS_GLOBAL);

        handleEndpointOverride(builder, endpointOverride);

        return builder.build();
    }

    private void handleBaseEndpointOverride(S3BaseClientBuilder<?, ?> builder, String endpointOverride) {
        URI endpointOverrideUri;

        if (StringUtils.isNotBlank(endpointOverride)) {
            endpointOverrideUri = URI.create(endpointOverride);
        } else {
            endpointOverrideUri = configuration.getEndpointOverride();
        }

        if (endpointOverrideUri != null) {
            builder.serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                    .endpointOverride(endpointOverrideUri);
        }
    }

    private void handleEndpointOverride(SdkClientBuilder<?, ?> builder, String endpointOverride) {

        // either take override from parameter, or from config, or null
        var uri = Optional.ofNullable(endpointOverride)
                .map(URI::create)
                .orElseGet(configuration::getEndpointOverride);

        builder.endpointOverride(uri);
    }
}
