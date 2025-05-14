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
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.junit.extensions.EmbeddedRuntime;
import org.eclipse.edc.junit.extensions.RuntimeExtension;
import org.eclipse.edc.junit.extensions.RuntimePerClassExtension;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
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

import java.util.HashMap;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.SECRET_ACCESS_ALIAS_PREFIX;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.IAM;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.STS;

@EndToEndTest
@Testcontainers
class S3ProvisionEndToEndTest {

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
                    .configurationProvider(S3ProvisionEndToEndTest::runtimeConfig));

    private S3BucketProvisioner provisioner;

    @BeforeEach
    void setUp() {
        LOCALSTACK_CONTAINER.start();
        var awsClientProviderConfiguration = AwsClientProviderConfiguration.Builder.newInstance()
                .credentialsProvider(() -> AwsBasicCredentials.create(LOCALSTACK_CONTAINER.getAccessKey(), LOCALSTACK_CONTAINER.getSecretKey()))
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3))
                .threadPoolSize(1)
                .build();

        var clientProvider = new AwsClientProviderImpl(awsClientProviderConfiguration);
        var provisionerConfiguration = new S3BucketProvisionerConfiguration(10, 1000);
        provisioner = new S3BucketProvisioner(clientProvider, monitor, vault, RETRY_POLICY, provisionerConfiguration);
    }

    @AfterEach
    void tearDown() {
        LOCALSTACK_CONTAINER.stop();
    }

    @Test
    void provision_createsBucketAndRole_withCredentialsFromDataDestination() {
        var resourceId = UUID.randomUUID().toString();
        var transferProcessId = UUID.randomUUID().toString();
        var resourceDefinition = S3BucketResourceDefinition.Builder.newInstance()
                .id(resourceId)
                .transferProcessId(transferProcessId)
                .regionId(LOCALSTACK_CONTAINER.getRegion())
                .accessKeyId(LOCALSTACK_CONTAINER.getAccessKey())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString())
                .bucketName(BUCKET_NAME)
                .build();
        when(vault.resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + resourceId)).thenReturn(LOCALSTACK_CONTAINER.getSecretKey());

        var response = provisioner.provision(resourceDefinition, Policy.Builder.newInstance().build()).join();

        assertNotNull(response);
        assertNull(response.getFailure());
        assertNotNull(response.getContent());
        assertNotNull(response.getContent().getResource());
        assertThat(response.getContent().isInProcess()).isFalse();
        assertThat(response.getContent().getResource()).isInstanceOf(S3BucketProvisionedResource.class);
        validateS3BucketProvisionedResource(response, resourceDefinition, resourceId, transferProcessId, LOCALSTACK_CONTAINER.getAccessKey());
        verify(vault).resolveSecret(SECRET_ACCESS_ALIAS_PREFIX + resourceId);
    }

    @Test
    void provision_createsBucketAndRole_withApplicationCredentials() {
        var resourceId = UUID.randomUUID().toString();
        var transferProcessId = UUID.randomUUID().toString();
        var resourceDefinition = S3BucketResourceDefinition.Builder.newInstance()
                .id(resourceId)
                .transferProcessId(transferProcessId)
                .regionId(LOCALSTACK_CONTAINER.getRegion())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString())
                .bucketName(BUCKET_NAME)
                .build();

        var response = provisioner.provision(resourceDefinition, Policy.Builder.newInstance().build()).join();

        assertNotNull(response);
        assertNull(response.getFailure());
        assertNotNull(response.getContent());
        assertNotNull(response.getContent().getResource());
        assertThat(response.getContent().isInProcess()).isFalse();
        assertThat(response.getContent().getResource()).isInstanceOf(S3BucketProvisionedResource.class);
        validateS3BucketProvisionedResource(response, resourceDefinition, resourceId, transferProcessId, null);
        verifyNoInteractions(vault);
    }

    private static void validateS3BucketProvisionedResource(
            StatusResult<ProvisionResponse> response,
            S3BucketResourceDefinition resourceDefinition,
            String resourceId,
            String transferProcessId,
            String accessKeyId) {
        var s3BucketProvisionedResource = (S3BucketProvisionedResource) response.getContent().getResource();
        assertNotNull(s3BucketProvisionedResource.getRole());
        assertThat(s3BucketProvisionedResource.getRole()).isEqualTo(resourceDefinition.getTransferProcessId());
        assertThat(s3BucketProvisionedResource.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(s3BucketProvisionedResource.getEndpointOverride()).isEqualTo(LOCALSTACK_CONTAINER.getEndpointOverride(S3).toString());
        assertThat(s3BucketProvisionedResource.getResourceDefinitionId()).isEqualTo(resourceId);
        assertThat(s3BucketProvisionedResource.getTransferProcessId()).isEqualTo(transferProcessId);
        assertThat(s3BucketProvisionedResource.getAccessKeyId()).isEqualTo(accessKeyId);
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