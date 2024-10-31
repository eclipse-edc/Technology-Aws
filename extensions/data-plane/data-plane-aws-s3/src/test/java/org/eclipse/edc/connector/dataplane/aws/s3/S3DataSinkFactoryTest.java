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
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *       ZF Friedrichshafen AG
 *       Cofinity-X - additional test for secret deserialization
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.junit.assertions.AbstractResultAssert.assertThat;
import static org.eclipse.edc.validator.spi.Violation.violation;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3DataSinkFactoryTest {

    private final AwsClientProvider clientProvider = mock();
    private final Vault vault = mock();
    private final TypeManager typeManager = new JacksonTypeManager();
    private final DataAddressValidatorRegistry validator = mock();

    private final S3DataSinkFactory factory = new S3DataSinkFactory(clientProvider, mock(), mock(),
            vault, typeManager.getMapper(), 1024, validator);

    @Test
    void canHandle_returnsTrueWhenExpectedType() {
        var dataAddress = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();

        var result = factory.canHandle(createRequest(dataAddress));

        assertThat(result).isTrue();
    }

    @Test
    void canHandle_returnsFalseWhenUnexpectedType() {
        var dataAddress = DataAddress.Builder.newInstance().type("any").build();

        var result = factory.canHandle(createRequest(dataAddress));

        assertThat(result).isFalse();
    }

    @Test
    void validate_shouldSucceed_whenValidatorSucceeds() {
        when(validator.validateDestination(any())).thenReturn(ValidationResult.success());
        var destination = TestFunctions.s3DataAddressWithCredentials();
        var request = createRequest(destination);

        var result = factory.validateRequest(request);

        assertThat(result).isSucceeded();
        verify(validator).validateDestination(destination);
    }

    @Test
    void validate_shouldFail_whenValidatorFails() {
        when(validator.validateDestination(any())).thenReturn(ValidationResult.failure(violation("error", "path")));
        var destination = TestFunctions.s3DataAddressWithCredentials();
        var request = createRequest(destination);

        var result = factory.validateRequest(request);

        assertThat(result).isFailed();
        verify(validator).validateDestination(destination);
    }
    
    @Test
    void createSink_shouldGetTheSecretTokenFromTheVault() {
        var destination = TestFunctions.s3DataAddressWithCredentials();
        var secretToken = new AwsSecretToken("accessKeyId", "secretAccessKey");
        when(vault.resolveSecret(destination.getKeyName())).thenReturn(typeManager.writeValueAsString(secretToken));
        when(validator.validateDestination(any())).thenReturn(ValidationResult.success());
        var request = createRequest(destination);
        
        var sink = factory.createSink(request);
        
        assertThat(sink).isNotNull().isInstanceOf(S3DataSink.class);
        var captor = ArgumentCaptor.forClass(S3ClientRequest.class);
        verify(clientProvider).s3Client(captor.capture());
        var s3ClientRequest = captor.getValue();
        assertThat(s3ClientRequest.region()).isEqualTo(TestFunctions.VALID_REGION);
        assertThat(s3ClientRequest.secretToken()).isInstanceOf(AwsSecretToken.class);
        assertThat(s3ClientRequest.endpointOverride()).isNull();
    }

    @Test
    void createSink_shouldGetTheTemporarySecretTokenFromTheVault() {
        var destination = TestFunctions.s3DataAddressWithCredentials();
        var temporaryKey = new AwsTemporarySecretToken("temporaryId", "temporarySecret", "temporaryToken", 10);
        when(vault.resolveSecret(destination.getKeyName())).thenReturn(typeManager.writeValueAsString(temporaryKey));
        when(validator.validateDestination(any())).thenReturn(ValidationResult.success());
        var request = createRequest(destination);

        var sink = factory.createSink(request);

        assertThat(sink).isNotNull().isInstanceOf(S3DataSink.class);
        var captor = ArgumentCaptor.forClass(S3ClientRequest.class);
        verify(clientProvider).s3Client(captor.capture());
        var s3ClientRequest = captor.getValue();
        assertThat(s3ClientRequest.region()).isEqualTo(TestFunctions.VALID_REGION);
        assertThat(s3ClientRequest.secretToken()).isInstanceOf(AwsTemporarySecretToken.class);
        assertThat(s3ClientRequest.endpointOverride()).isNull();
    }

    @Test
    void createSink_shouldCreateDataSinkWithCredentialsInDataAddressIfTheresNoSecret() {
        when(vault.resolveSecret(any())).thenReturn(null);
        when(validator.validateDestination(any())).thenReturn(ValidationResult.success());
        var destination = TestFunctions.s3DataAddressWithCredentials();
        var request = createRequest(destination);

        var sink = factory.createSink(request);

        assertThat(sink).isNotNull().isInstanceOf(S3DataSink.class);
        var captor = ArgumentCaptor.forClass(S3ClientRequest.class);
        verify(clientProvider).s3Client(captor.capture());
        var s3ClientRequest = captor.getValue();
        assertThat(s3ClientRequest.region()).isEqualTo(TestFunctions.VALID_REGION);
        assertThat(s3ClientRequest.secretToken()).isEqualTo(new AwsSecretToken(TestFunctions.VALID_ACCESS_KEY_ID, TestFunctions.VALID_SECRET_ACCESS_KEY));
        assertThat(s3ClientRequest.endpointOverride()).isNull();
    }

    @Test
    void createSink_shouldLetTheProviderGetTheCredentialsAsFallback() {
        when(vault.resolveSecret(any())).thenReturn(null);
        when(validator.validateDestination(any())).thenReturn(ValidationResult.success());
        var destination = TestFunctions.s3DataAddressWithoutCredentials();
        var request = createRequest(destination);

        var sink = factory.createSink(request);

        assertThat(sink).isNotNull().isInstanceOf(S3DataSink.class);
        var captor = ArgumentCaptor.forClass(S3ClientRequest.class);
        verify(clientProvider).s3Client(captor.capture());
        var s3ClientRequest = captor.getValue();
        assertThat(s3ClientRequest.region()).isEqualTo(TestFunctions.VALID_REGION);
        assertThat(s3ClientRequest.secretToken()).isNull();
        assertThat(s3ClientRequest.endpointOverride()).isNull();
    }

    @Test
    void createSink_shouldThrowExceptionIfValidationFails() {
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .build();
        when(validator.validateDestination(any())).thenReturn(ValidationResult.failure(violation("error", "path")));

        var request = createRequest(destination);

        assertThatThrownBy(() -> factory.createSink(request)).isInstanceOf(EdcException.class);
    }

    private DataFlowStartMessage createRequest(DataAddress destination) {
        return DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build())
                .destinationDataAddress(destination)
                .build();
    }

}
