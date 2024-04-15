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
 *       ZF Friedrichshafen AG - Initial implementation
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3BucketSchema;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;

import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.connector.dataplane.aws.s3.TestFunctions.VALID_ACCESS_KEY_ID;
import static org.eclipse.edc.connector.dataplane.aws.s3.TestFunctions.VALID_BUCKET_NAME;
import static org.eclipse.edc.connector.dataplane.aws.s3.TestFunctions.VALID_REGION;
import static org.eclipse.edc.connector.dataplane.aws.s3.TestFunctions.VALID_SECRET_ACCESS_KEY;
import static org.eclipse.edc.connector.dataplane.aws.s3.TestFunctions.s3DataAddressWithCredentials;
import static org.eclipse.edc.connector.dataplane.aws.s3.TestFunctions.s3DataAddressWithoutCredentials;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3DataSourceFactoryTest {

    private final AwsClientProvider clientProvider = mock(AwsClientProvider.class);
    private final TypeManager typeManager = new JacksonTypeManager();
    private final Vault vault = mock(Vault.class);
    private final S3DataSourceFactory factory = new S3DataSourceFactory(clientProvider, mock(Monitor.class), vault, typeManager);
    private final ArgumentCaptor<S3ClientRequest> s3ClientRequestArgumentCaptor = ArgumentCaptor.forClass(S3ClientRequest.class);

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
    void validate_shouldSucceedIfPropertiesAreValid() {
        var source = s3DataAddressWithCredentials();
        var request = createRequest(source);

        var result = factory.validateRequest(request);

        assertThat(result.succeeded()).isTrue();
    }

    @ParameterizedTest
    @ArgumentsSource(InvalidInputs.class)
    void validate_shouldFailIfMandatoryPropertiesAreMissing(String bucketName, String region, String accessKeyId, String secretAccessKey) {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(S3BucketSchema.BUCKET_NAME, bucketName)
                .property(S3BucketSchema.REGION, region)
                .property(S3BucketSchema.ACCESS_KEY_ID, accessKeyId)
                .property(S3BucketSchema.SECRET_ACCESS_KEY, secretAccessKey)
                .build();

        var request = createRequest(source);

        var result = factory.validateRequest(request);

        assertThat(result.failed()).isTrue();
    }

    @Test
    void createSource_shouldCreateDataSource() {
        DataAddress source = s3DataAddressWithCredentials();
        var request = createRequest(source);

        var dataSource = factory.createSource(request);

        assertThat(dataSource).isNotNull().isInstanceOf(S3DataSource.class);
    }

    @Test
    void createSink_shouldLetTheProviderGetTheCredentialsIfNotProvidedByTheAddress() {
        var destination = s3DataAddressWithoutCredentials();
        var request = createRequest(destination);

        var sink = factory.createSource(request);

        assertThat(sink).isNotNull().isInstanceOf(S3DataSource.class);

        verify(clientProvider).s3Client(s3ClientRequestArgumentCaptor.capture());

        S3ClientRequest s3ClientRequest = s3ClientRequestArgumentCaptor.getValue();

        assertThat(s3ClientRequest.region()).isEqualTo(TestFunctions.VALID_REGION);
        assertThat(s3ClientRequest.secretToken()).isNull();
        assertThat(s3ClientRequest.endpointOverride()).isNull();
    }

    @Test
    void createSource_shouldThrowExceptionIfValidationFails() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .build();

        var request = createRequest(source);

        assertThatThrownBy(() -> factory.createSource(request)).isInstanceOf(EdcException.class);
    }

    @Test
    void createSource_shouldGetTheSecretTokenFromTheVault() {
        var source = TestFunctions.s3DataAddressWithCredentials();
        var temporaryKey = new AwsTemporarySecretToken("temporaryId", "temporarySecret", null, 0);
        when(vault.resolveSecret(source.getKeyName())).thenReturn(typeManager.writeValueAsString(temporaryKey));
        var request = createRequest(source);

        var s3Source = factory.createSource(request);

        assertThat(s3Source).isNotNull().isInstanceOf(S3DataSource.class);

        verify(clientProvider).s3Client(s3ClientRequestArgumentCaptor.capture());

        S3ClientRequest s3ClientRequest = s3ClientRequestArgumentCaptor.getValue();

        assertThat(s3ClientRequest.region()).isEqualTo(TestFunctions.VALID_REGION);
        assertThat(s3ClientRequest.secretToken()).isInstanceOf(AwsTemporarySecretToken.class);
        assertThat(s3ClientRequest.endpointOverride()).isNull();
    }

    private DataFlowStartMessage createRequest(DataAddress source) {
        return DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(source)
                .destinationDataAddress(DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build())
                .build();
    }

    private static class InvalidInputs implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(VALID_BUCKET_NAME, " ", VALID_ACCESS_KEY_ID, VALID_SECRET_ACCESS_KEY),
                    Arguments.of(" ", VALID_REGION, VALID_ACCESS_KEY_ID, VALID_SECRET_ACCESS_KEY)
            );
        }
    }
}
