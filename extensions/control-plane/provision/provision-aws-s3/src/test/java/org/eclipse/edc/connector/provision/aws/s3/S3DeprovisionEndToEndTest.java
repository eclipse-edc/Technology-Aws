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
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.configuration.Config;
import org.eclipse.edc.spi.system.configuration.ConfigFactory;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.SECRET_ACCESS_ALIAS_PREFIX;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.IAM;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.STS;

@EndToEndTest
@Testcontainers
class S3DeprovisionEndToEndTest {

    private static final DockerImageName LOCALSTACK_DOCKER_IMAGE = DockerImageName.parse("localstack/localstack:4.2.0");

    private static final RetryPolicy<Object> RETRY_POLICY = RetryPolicy.ofDefaults();
    private static final String BUCKET_NAME = "test-bucket";
    private final Vault vault = mock();
    private final Monitor monitor = mock();

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
                    .configurationProvider(S3DeprovisionEndToEndTest::runtimeConfig));

    private S3BucketProvisioner s3BucketProvisioner;

    @BeforeEach
    void setUp() {
        LOCALSTACK_CONTAINER.start();
        var credentialsProvider = AwsBasicCredentials.create(LOCALSTACK_CONTAINER.getAccessKey(), LOCALSTACK_CONTAINER.getSecretKey());
        var awsClientProviderConfiguration = AwsClientProviderConfiguration.Builder.newInstance()
                .credentialsProvider(() -> credentialsProvider)
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3))
                .threadPoolSize(1)
                .build();

        var clientProvider = new AwsClientProviderImpl(awsClientProviderConfiguration);

        var s3Client = getS3Client(StaticCredentialsProvider.create(credentialsProvider));
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(BUCKET_NAME)
                .build());
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key("sourceObjectName")
                .build(), RequestBody.fromBytes("fileContent".getBytes()));


        var provisionerConfiguration = new S3BucketProvisionerConfiguration(10, 1000);
        s3BucketProvisioner = new S3BucketProvisioner(
                clientProvider, monitor, vault, RETRY_POLICY, provisionerConfiguration);
    }

    @AfterEach
    void tearDown() {
        LOCALSTACK_CONTAINER.stop();
    }

    @Test
    void deprovision_createsBucketAndRole_withCredentialsFromDataDestination() {
        var transferProcessId = UUID.randomUUID().toString();
        var provisionedResource = provisionTestResource(transferProcessId, true);
        var deprovisionResourceId = UUID.randomUUID().toString();
        var deprovisionResourceDefinition = S3BucketProvisionedResource.Builder.newInstance()
                .id(deprovisionResourceId)
                .transferProcessId(transferProcessId)
                .hasToken(true)
                .role(provisionedResource.getRole())
                .accessKeyId(LOCALSTACK_CONTAINER.getAccessKey())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString())
                .bucketName(BUCKET_NAME)
                .resourceDefinitionId(deprovisionResourceId)
                .resourceName("test-name")
                .dataAddress(DataAddress.Builder.newInstance()
                        .type("s3")
                        .property("region", LOCALSTACK_CONTAINER.getRegion())
                        .property("bucketName", BUCKET_NAME)
                        .property("endpointOverride", LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString())
                        .build())
                .build();
        when(vault.resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + deprovisionResourceId)).thenReturn(LOCALSTACK_CONTAINER.getSecretKey());

        var deprovisionResponse = s3BucketProvisioner.deprovision(
                deprovisionResourceDefinition, Policy.Builder.newInstance().build()).join();

        assertNotNull(deprovisionResponse);
        assertNull(deprovisionResponse.getFailure());
        assertNotNull(deprovisionResponse.getContent());
        assertThat(deprovisionResponse.getContent().isError()).isFalse();
        assertThat(deprovisionResponse.getContent().getProvisionedResourceId()).isEqualTo(deprovisionResourceId);
        assertNull(deprovisionResponse.getContent().getErrorMessage());
        assertThat(deprovisionResponse.getContent().isInProcess()).isFalse();
        verify(vault).resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + deprovisionResourceId);
    }

    @Test
    void deprovision_createsBucketAndRole_withApplicationCredentials() {
        var transferProcessId = UUID.randomUUID().toString();
        var provisionedResource = provisionTestResource(transferProcessId, false);
        var deprovisionResourceId = UUID.randomUUID().toString();
        var deprovisionResourceDefinition = S3BucketProvisionedResource.Builder.newInstance()
                .id(deprovisionResourceId)
                .transferProcessId(transferProcessId)
                .hasToken(true)
                .role(provisionedResource.getRole())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString())
                .bucketName(BUCKET_NAME)
                .resourceDefinitionId(deprovisionResourceId)
                .resourceName("test-name")
                .dataAddress(DataAddress.Builder.newInstance()
                        .type("s3")
                        .property("region", LOCALSTACK_CONTAINER.getRegion())
                        .property("bucketName", BUCKET_NAME)
                        .property("endpointOverride", LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString())
                        .build())
                .build();

        var deprovisionResponse = s3BucketProvisioner.deprovision(
                deprovisionResourceDefinition, Policy.Builder.newInstance().build()).join();

        assertNotNull(deprovisionResponse);
        assertNull(deprovisionResponse.getFailure());
        assertNotNull(deprovisionResponse.getContent());
        assertThat(deprovisionResponse.getContent().isError()).isFalse();
        assertThat(deprovisionResponse.getContent().getProvisionedResourceId()).isEqualTo(deprovisionResourceId);
        assertNull(deprovisionResponse.getContent().getErrorMessage());
        assertThat(deprovisionResponse.getContent().isInProcess()).isFalse();
        verify(vault, times(0)).resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + deprovisionResourceId);
    }

    private S3BucketProvisionedResource provisionTestResource(String transferProcessId, boolean hasResourceCredentials) {
        var resourceId = UUID.randomUUID().toString();
        var resourceDefinition = S3BucketResourceDefinition.Builder.newInstance()
                .id(resourceId)
                .transferProcessId(transferProcessId)
                .regionId(LOCALSTACK_CONTAINER.getRegion())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString())
                .bucketName(BUCKET_NAME);

        if (hasResourceCredentials) {
            resourceDefinition.accessKeyId(LOCALSTACK_CONTAINER.getAccessKey());
            when(vault.resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + resourceId)).thenReturn(LOCALSTACK_CONTAINER.getSecretKey());
        }

        var result = s3BucketProvisioner.provision(resourceDefinition.build(), Policy.Builder.newInstance().build()).join();
        return (S3BucketProvisionedResource) result.getContent().getResource();
    }

    private S3Client getS3Client(AwsCredentialsProvider credentialsProvider) {
        return S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(LOCALSTACK_CONTAINER.getRegion()))
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpoint())
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
                put("edc.aws.access.key", "test-key");
                put("edc.aws.secret.access.key", "secret-key");
            }
        };

        return ConfigFactory.fromMap(settings);
    }
}