/*
 *  Copyright (c) 2025 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *       ZF Friedrichshafen AG - Initial implementation
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import dev.failsafe.RetryPolicy;
import org.eclipse.edc.aws.s3.AwsClientProviderConfiguration;
import org.eclipse.edc.aws.s3.AwsClientProviderImpl;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProvisionManager;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ResourceManifestGenerator;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.junit.extensions.EmbeddedRuntime;
import org.eclipse.edc.junit.extensions.RuntimeExtension;
import org.eclipse.edc.junit.extensions.RuntimePerClassExtension;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.configuration.Config;
import org.eclipse.edc.spi.system.configuration.ConfigFactory;
import org.eclipse.edc.spi.types.TypeManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.IAM;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.STS;

@EndToEndTest
@Testcontainers
class S3ProvisionEndToEndTest {

    private static final DockerImageName LOCALSTACK_DOCKER_IMAGE = DockerImageName.parse("localstack/localstack:4.2.0");

    private static final RetryPolicy<Object> RETRY_POLICY = RetryPolicy.ofDefaults();

    @Container
    private static final LocalStackContainer LOCALSTACK_CONTAINER = new LocalStackContainer(LOCALSTACK_DOCKER_IMAGE)
            .withServices(S3, IAM, STS);

    @RegisterExtension
    private static final RuntimeExtension RUNTIME = new RuntimePerClassExtension(
            new EmbeddedRuntime("runtime", ":extensions:control-plane:provision:provision-aws-s3")
                    .registerServiceMock(TypeManager.class, new JacksonTypeManager())
                    .registerServiceMock(ProvisionManager.class, mock(ProvisionManager.class))
                    .registerServiceMock(RetryPolicy.class, RETRY_POLICY)
                    .registerServiceMock(ResourceManifestGenerator.class, mock(ResourceManifestGenerator.class))
                    .configurationProvider(S3ProvisionEndToEndTest::runtimeConfig));

    private S3ProvisionPipeline pipeline;
    private final Vault vault = mock(Vault.class);
    private final Monitor monitor = mock(Monitor.class);

    @BeforeEach
    void setUp() {
        LOCALSTACK_CONTAINER.start();
        var awsClientProviderConfiguration = AwsClientProviderConfiguration.Builder.newInstance()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3))
                .threadPoolSize(10)
                .build();

        var clientProvider = new AwsClientProviderImpl(awsClientProviderConfiguration);
        var s3Client = getS3Client();
        var iamClient = getIamClient();
        when(vault.resolveSecret(anyString())).thenReturn(LOCALSTACK_CONTAINER.getSecretKey());

        pipeline = S3ProvisionPipeline.Builder
                .newInstance(RETRY_POLICY)
                .clientProvider(clientProvider)
                .monitor(monitor)
                .vault(vault)
                .roleMaxSessionDuration(1000)
                .build();
    }

    @AfterEach
    void tearDown() {
        LOCALSTACK_CONTAINER.stop();
    }

    @Test
    void provision_createsBucketAndRole() {
        var resourceDefinition = S3BucketResourceDefinition.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .transferProcessId(UUID.randomUUID().toString())
                .regionId(LOCALSTACK_CONTAINER.getRegion())
                .accessKeyId(LOCALSTACK_CONTAINER.getAccessKey())
                .bucketName("test-bucket")
                .build();

        CompletableFuture<S3ProvisionResponse> responseFuture = pipeline.provision(resourceDefinition);

        S3ProvisionResponse response = responseFuture.join(); // todo: maybe not this join

        assertNotNull(response);
        assertNotNull(response.getRole());
        assertNotNull(response.getCredentials());
        assertEquals(resourceDefinition.getTransferProcessId(), response.getRole().roleName());
    }

    private StaticCredentialsProvider localStackCredentials() {
        return StaticCredentialsProvider.create(AwsBasicCredentials
                .create(LOCALSTACK_CONTAINER.getAccessKey(), LOCALSTACK_CONTAINER.getSecretKey()));
    }

    private S3Client getS3Client() {
        return S3Client.builder()
                .credentialsProvider(localStackCredentials())
                .region(Region.of(LOCALSTACK_CONTAINER.getRegion()))
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK_CONTAINER.getAccessKey(), LOCALSTACK_CONTAINER.getSecretKey())))
                .build();
    }

    private IamClient getIamClient() {
        return IamClient.builder()
                .credentialsProvider(localStackCredentials())
                .region(Region.AWS_GLOBAL)
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(LOCALSTACK_CONTAINER.getAccessKey(), LOCALSTACK_CONTAINER.getSecretKey())))
                .build();
    }

    private static Config runtimeConfig() {
        var settings = new HashMap<String, String>() {
            {
                put("edc.participant.id", "runtime");
                put("web.http.port", "8080");
                put("web.http.path", "/api");
                put("web.http.management.port", "8181");
                put("web.http.management.path", "/management");
                put("web.http.protocol.port", "8282");
                put("web.http.protocol.path", "/protocol");
                put("web.http.control.port", "8383");
                put("web.http.control.path", "/control");
                put("web.http.public.port", "8585");
                put("web.http.public.path", "/public");
                put("edc.api.auth.key", "password");
                put("edc.control.endpoint", "http://localhost:8383/control");
                put("edc.dsp.callback.address", "http://localhost:8282/protocol");
                put("edc.dataplane.api.public.baseurl", "http://localhost:8585/public");
                put("edc.dpf.selector.url", "http://localhost:8383/control/v1/dataplanes");
                put("edc.transfer.proxy.token.signer.privatekey.alias", "private-key");
                put("edc.transfer.proxy.token.verifier.publickey.alias", "public-key");
            }
        };

        return ConfigFactory.fromMap(settings);
    }
}