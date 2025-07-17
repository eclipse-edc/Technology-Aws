/*
 *  Copyright (c) 2022 ZF Friedrichshafen AG
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       ZF Friedrichshafen AG - Initial API and Implementation
 *       SAP SE - refactoring
 *       Cofinity-X - fix exception in canGenerate for PULL transfers
 *
 */

package org.eclipse.edc.connector.provision.aws.s3;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;


public class S3ConsumerResourceDefinitionGeneratorTest {

    private S3ConsumerResourceDefinitionGenerator generator;
    private final Vault vault = mock();

    @BeforeEach
    void setUp() {
        generator = new S3ConsumerResourceDefinitionGenerator(vault);
    }

    @Test
    void generate() {
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                .property(S3BucketSchema.BUCKET_NAME, "test-name")
                .property(S3BucketSchema.REGION, Region.EU_WEST_2.id())
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.generate(transferProcess, policy);

        assertThat(definition).isInstanceOf(S3BucketResourceDefinition.class);
        var objectDef = (S3BucketResourceDefinition) definition;
        assertThat(objectDef.getBucketName()).isEqualTo("test-name");
        assertThat(objectDef.getRegionId()).isEqualTo(Region.EU_WEST_2.id());
        assertThat(objectDef.getId()).satisfies(UUID::fromString);
        assertThat(objectDef.getAccessKeyId()).isNull();
        verifyNoInteractions(vault);
    }

    @Test
    void generate_noDataRequestAsParameter() {
        var policy = Policy.Builder.newInstance().build();
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(null, policy));
    }

    @Test
    void generate_noRegionSpecified() {
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                .property(S3BucketSchema.BUCKET_NAME, "test-name")
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.generate(transferProcess, policy);

        assertThat(definition).isInstanceOf(S3BucketResourceDefinition.class);
        var objectDef = (S3BucketResourceDefinition) definition;
        assertThat(objectDef.getBucketName()).isEqualTo("test-name");
        assertThat(objectDef.getRegionId()).isEqualTo(Region.US_EAST_1.id());
        assertThat(objectDef.getId()).satisfies(UUID::fromString);
    }

    @Test
    void generate_noBucketNameSpecified() {
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                .property(S3BucketSchema.REGION, Region.EU_WEST_2.id())
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(transferProcess, policy));
    }

    @Test
    void generate_withCredentials() {
        var accessKeyId = "test-access-key";
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                .property(S3BucketSchema.BUCKET_NAME, "test-name")
                .property(S3BucketSchema.REGION, Region.EU_WEST_2.id())
                .property(S3BucketSchema.ACCESS_KEY_ID, accessKeyId)
                .property(S3BucketSchema.SECRET_ACCESS_KEY, "test-secret-access-key")
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.generate(transferProcess, policy);

        assertThat(definition).isInstanceOf(S3BucketResourceDefinition.class);
        var objectDef = (S3BucketResourceDefinition) definition;
        assertThat(objectDef.getBucketName()).isEqualTo("test-name");
        assertThat(objectDef.getRegionId()).isEqualTo(Region.EU_WEST_2.id());
        assertThat(objectDef.getId()).satisfies(UUID::fromString);
        assertThat(objectDef.getAccessKeyId()).isEqualTo(accessKeyId);
        verify(vault).storeSecret(anyString(), anyString());
    }

    @Test
    void canGenerate() {
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE)
                .property(S3BucketSchema.BUCKET_NAME, "test-name")
                .property(S3BucketSchema.REGION, Region.US_EAST_1.id())
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.canGenerate(transferProcess, policy);

        assertThat(definition).isTrue();
    }

    @Test
    void canGenerateIsNotTypeS3BucketSchema() {
        var destination = DataAddress.Builder.newInstance().type("aNonS3BucketSchema")
                .property(S3BucketSchema.BUCKET_NAME, "test-name")
                .property(S3BucketSchema.REGION, Region.US_EAST_1.id())
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.canGenerate(transferProcess, policy);
        assertThat(definition).isFalse();
    }

    @Test
    void canGenerate_dataDestinationNull_shouldReturnFalse() {
        var transferProcess = TransferProcess.Builder.newInstance()
                .assetId("asset-id")
                .contractId("contract-id")
                .build();
        var policy = Policy.Builder.newInstance().build();
        
        var definition = generator.canGenerate(transferProcess, policy);
        assertThat(definition).isFalse();
    }
}
