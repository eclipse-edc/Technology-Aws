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

package org.eclipse.edc.connector.dataplane.aws.s3.copy;

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Violation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AwsS3CopyTransferServiceTest {
    
    private AwsS3CopyTransferService transferService;
    
    private final AwsClientProvider clientProvider = mock(AwsClientProvider.class);
    private final S3AsyncClient s3Client = mock(S3AsyncClient.class);
    private final Vault vault = mock(Vault.class);
    private final TypeManager typeManager = mock(TypeManager.class);
    private final DataAddressValidatorRegistry validatorRegistry = mock(DataAddressValidatorRegistry.class);
    private final Monitor monitor = mock(Monitor.class);
    
    private final String endpoint = "https://endpoint";
    private final String sourceRegion = "sourceRegion";
    private final String sourceBucket = "sourceBucket";
    private final String sourceObject = "sourceObject";
    private final String keyName = "keyName";
    private final String destinationBucket = "destinationBucket";
    private final String destinationObject = "destinationObject";
    
    @BeforeEach
    void setUp() {
        transferService = new AwsS3CopyTransferService(clientProvider, vault, typeManager, validatorRegistry, monitor, 500);
    }
    
    @Test
    void canHandle_sameTypeAndNoEndpointOverride_shouldReturnTrue() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.canHandle(request);
        
        assertThat(result).isTrue();
    }
    
    @Test
    void canHandle_notSameType_shouldReturnFalse() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance().type("something else").build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.canHandle(request);
        
        assertThat(result).isFalse();
    }
    
    @Test
    void canHandle_sameTypeAndSameEndpointOverride_shouldReturnTrue() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.canHandle(request);
        
        assertThat(result).isTrue();
    }
    
    @Test
    void canHandle_sameTypeAndNotSameEndpointOverride_shouldReturnFalse() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, "http://some-other-endpoint")
                .build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.canHandle(request);
        
        assertThat(result).isFalse();
    }
    
    @Test
    void validate_validRequest_shouldReturnSuccess() {
        when(validatorRegistry.validateSource(any())).thenReturn(ValidationResult.success());
        when(validatorRegistry.validateDestination(any())).thenReturn(ValidationResult.success());
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenReturn(temporarySecretToken());
    
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(keyName)
                .build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var request = dataFlowStartMessage(source, destination);
        
        var result = transferService.validate(request);
        
        assertThat(result.succeeded()).isTrue();
    }
    
    @Test
    void validate_invalidSource_shouldReturnFalse() {
        var violation = Violation.violation("error", "path");
        when(validatorRegistry.validateSource(any())).thenReturn(ValidationResult.failure(violation));
        when(validatorRegistry.validateDestination(any())).thenReturn(ValidationResult.success());
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenReturn(temporarySecretToken());
    
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(keyName)
                .build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.validate(request);
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void validate_invalidDestination_shouldReturnFalse() {
        var violation = Violation.violation("error", "path");
        when(validatorRegistry.validateSource(any())).thenReturn(ValidationResult.success());
        when(validatorRegistry.validateDestination(any())).thenReturn(ValidationResult.failure(violation));
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenReturn(temporarySecretToken());
    
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(keyName)
                .build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.validate(request);
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void validate_missingSourceKeyName_shouldReturnFalse() {
        when(validatorRegistry.validateSource(any())).thenReturn(ValidationResult.success());
        when(validatorRegistry.validateDestination(any())).thenReturn(ValidationResult.success());
        
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var request = dataFlowStartMessage(source, destination);
        
        var result = transferService.validate(request);
        
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void validate_missingSourceCredentials_shouldReturnFalse() {
        when(validatorRegistry.validateSource(any())).thenReturn(ValidationResult.success());
        when(validatorRegistry.validateDestination(any())).thenReturn(ValidationResult.success());
        when(vault.resolveSecret(keyName)).thenReturn(null);
    
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(keyName)
                .build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.validate(request);
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void validate_invalidDestinationCredentials_shouldReturnFalse() {
        when(validatorRegistry.validateSource(any())).thenReturn(ValidationResult.success());
        when(validatorRegistry.validateDestination(any())).thenReturn(ValidationResult.success());
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenThrow(new EdcException("error"));
    
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(keyName)
                .build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.validate(request);
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void transfer_copyingObjectSuccessful_shouldReturnSuccess() throws Exception {
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenReturn(temporarySecretToken());
        when(clientProvider.s3AsyncClient(any(S3ClientRequest.class))).thenReturn(s3Client);
        
        var copyObjectResponse = CopyObjectResponse.builder().build();
        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenReturn(completedFuture(copyObjectResponse));

        var source = sourceDataAddress();
        var destination = destinationDataAddress();
        var request = dataFlowStartMessage(source, destination);
        
        var result = transferService.transfer(request).get();
        
        assertThat(result.succeeded()).isTrue();
        verify(s3Client).copyObject(argThat((CopyObjectRequest copyRequest) -> copyRequest.sourceBucket().equals(sourceBucket) &&
            copyRequest.sourceKey().equals(sourceObject) &&
            copyRequest.destinationBucket().equals(destinationBucket) &&
            copyRequest.destinationKey().equals(destinationObject))
        );
    }
    
    @Test
    void transfer_missingDestinationObjectName_shouldReturnSuccess() throws Exception {
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenReturn(temporarySecretToken());
        when(clientProvider.s3AsyncClient(any(S3ClientRequest.class))).thenReturn(s3Client);
        
        var copyObjectResponse = CopyObjectResponse.builder().build();
        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenReturn(completedFuture(copyObjectResponse));
        
        var source = sourceDataAddress();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, "destinationRegion")
                .property(BUCKET_NAME, destinationBucket)
                .build();
        var request = dataFlowStartMessage(source, destination);
        
        var result = transferService.transfer(request).get();
        
        assertThat(result.succeeded()).isTrue();
        verify(s3Client).copyObject(argThat((CopyObjectRequest copyRequest) -> copyRequest.sourceBucket().equals(sourceBucket) &&
                copyRequest.sourceKey().equals(sourceObject) &&
                copyRequest.destinationBucket().equals(destinationBucket) &&
                copyRequest.destinationKey().equals(sourceObject))
        );
    }
    
    @Test
    void transfer_errorCopyingObject_shouldReturnError() throws Exception {
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenReturn(temporarySecretToken());
        when(clientProvider.s3AsyncClient(any(S3ClientRequest.class))).thenReturn(s3Client);
        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenReturn(failedFuture(new RuntimeException("error")));
    
        var source = sourceDataAddress();
        var destination = destinationDataAddress();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.transfer(request).get();
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void transfer_missingKeyName_shouldReturnError() throws Exception {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, sourceRegion)
                .property(BUCKET_NAME, sourceBucket)
                .property(OBJECT_NAME, sourceObject)
                .build();
        var destination = destinationDataAddress();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.transfer(request).get();
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void transfer_missingSecret_shouldReturnError() throws Exception {
        when(vault.resolveSecret(keyName)).thenReturn(null);
    
        var source = sourceDataAddress();
        var destination = destinationDataAddress();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.transfer(request).get();
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void transfer_invalidSecret_shouldReturnError() throws Exception {
        when(vault.resolveSecret(keyName)).thenReturn("value");
        when(typeManager.readValue(anyString(), eq(AwsTemporarySecretToken.class))).thenThrow(new EdcException("error"));
    
        var source = sourceDataAddress();
        var destination = destinationDataAddress();
        var request = dataFlowStartMessage(source, destination);
    
        var result = transferService.transfer(request).get();
    
        assertThat(result.succeeded()).isFalse();
    }
    
    @Test
    void transfer_withSeparateSink_shouldThrowException() {
        var source = sourceDataAddress();
        var destination = destinationDataAddress();
        var request = dataFlowStartMessage(source, destination);
        
        var sink = mock(DataSink.class);
        
        assertThatThrownBy(() -> transferService.transfer(request, sink))
                .isInstanceOf(UnsupportedOperationException.class);
    }
    
    private DataFlowStartMessage dataFlowStartMessage(DataAddress source, DataAddress destination) {
        return DataFlowStartMessage.Builder.newInstance()
                .processId("transfer-process-id")
                .sourceDataAddress(source)
                .destinationDataAddress(destination)
                .build();
    }
    
    private AwsTemporarySecretToken temporarySecretToken() {
        return new AwsTemporarySecretToken("accessKeyId", "secretAccessKey", "sessionToken", 1234L);
    }
    
    private DataAddress sourceDataAddress() {
        return DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .keyName(keyName)
                .property(REGION, sourceRegion)
                .property(BUCKET_NAME, sourceBucket)
                .property(OBJECT_NAME, sourceObject)
                .build();
    }
    
    private DataAddress destinationDataAddress() {
        return DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, "destinationRegion")
                .property(BUCKET_NAME, destinationBucket)
                .property(OBJECT_NAME, destinationObject)
                .build();
    }
}
