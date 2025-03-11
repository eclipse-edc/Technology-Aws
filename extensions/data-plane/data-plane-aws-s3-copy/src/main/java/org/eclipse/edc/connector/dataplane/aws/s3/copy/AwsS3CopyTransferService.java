/*
 *  Copyright (c) 2025 Cofinity-X
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
import org.eclipse.edc.connector.controlplane.transfer.spi.types.SecretToken;
import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.spi.pipeline.TransferService;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.util.string.StringUtils;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Violation;
import software.amazon.awssdk.services.s3.internal.multipart.MultipartS3AsyncClient;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.multipart.MultipartConfiguration;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.FOLDER_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;

/**
 * Service for executing S3-to-S3 transfers within the AWS infrastructure. Initiates a cross-account
 * copy of S3 objects between source and destination as a multipart copy operation.
 */
public class AwsS3CopyTransferService implements TransferService {
    
    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final TypeManager typeManager;
    private final DataAddressValidatorRegistry validator;
    private final Monitor monitor;
    
    private final MultipartConfiguration multipartConfiguration;
    
    public AwsS3CopyTransferService(AwsClientProvider clientProvider, Vault vault,
                                    TypeManager typeManager, DataAddressValidatorRegistry validator,
                                    Monitor monitor, int chunkSizeInMb) {
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.vault = vault;
        this.typeManager = typeManager;
        this.validator = validator;
        this.multipartConfiguration = MultipartConfiguration.builder()
                .thresholdInBytes(chunkSizeInMb * 1024 * 1024L)
                .minimumPartSizeInBytes(chunkSizeInMb * 1024 * 1024L)
                .build();
    }
    
    @Override
    public boolean canHandle(DataFlowStartMessage request) {
        var sourceType = request.getSourceDataAddress().getType();
        var sinkType = request.getDestinationDataAddress().getType();
        var sourceEndpointOverride = request.getSourceDataAddress().getStringProperty(ENDPOINT_OVERRIDE);
        var destinationEndpointOverride = request.getDestinationDataAddress().getStringProperty(ENDPOINT_OVERRIDE);
        
        // only applicable for S3-to-S3 transfer
        var isSameType = S3BucketSchema.TYPE.equals(sourceType) && S3BucketSchema.TYPE.equals(sinkType);
        
        // if endpointOverride set, it needs to be the same for both source & destination
        var hasSameEndpointOverride = sameEndpointOverride(sourceEndpointOverride, destinationEndpointOverride);
        
        return isSameType && hasSameEndpointOverride;
    }
    
    @Override
    public Result<Boolean> validate(DataFlowStartMessage request) {
        var source = request.getSourceDataAddress();
        var sourceResult = validator.validateSource(source);
        var sourceCredentialsResult = validateSourceCredentials(source);
        
        var destination = request.getDestinationDataAddress();
        var destinationResult = validator.validateDestination(destination);
        
        var errors = Stream.of(sourceResult, sourceCredentialsResult, destinationResult)
                .filter(ValidationResult::failed)
                .map(ValidationResult::getFailureMessages)
                .flatMap(List::stream)
                .toList();
        
        return errors.isEmpty() ? Result.success(true) : Result.failure(errors);
    }
    
    private ValidationResult validateSourceCredentials(DataAddress source) {
        if (getCredentials(source) != null) {
            return ValidationResult.success();
        }
        
        var violation = Violation.violation("No or invalid credential found in vault for given key.", "keyName", source.getKeyName());
        return ValidationResult.failure(violation);
    }
    
    @Override
    public CompletableFuture<StreamResult<Object>> transfer(DataFlowStartMessage request) {
        var source = request.getSourceDataAddress();
        var sourceBucketName = source.getStringProperty(BUCKET_NAME);
        var sourceKey = source.getStringProperty(OBJECT_NAME);
        
        var destination = request.getDestinationDataAddress();
        var destinationRegion = destination.getStringProperty(REGION);
        var destinationBucketName = destination.getStringProperty(BUCKET_NAME);
        var destinationFolder = destination.getStringProperty(FOLDER_NAME);
        var destinationKey = destination.getStringProperty(OBJECT_NAME) != null ?
                destination.getStringProperty(OBJECT_NAME) : sourceKey;
        
        var secretToken = getCredentials(source);
        if (secretToken == null) {
            return completedFuture(StreamResult.error("Missing or invalid credentials."));
        }
        
        var s3ClientRequest = S3ClientRequest.from(destinationRegion, request.getDestinationDataAddress().getStringProperty(ENDPOINT_OVERRIDE), secretToken);
        var s3Client = clientProvider.s3AsyncClient(s3ClientRequest);
        var multipartClient = MultipartS3AsyncClient.create(s3Client, multipartConfiguration, true);
        
        var destinationFileName = getDestinationFileName(destinationKey, destinationFolder);
        
        var copyRequest = CopyObjectRequest.builder()
                .sourceBucket(sourceBucketName)
                .sourceKey(sourceKey)
                .destinationBucket(destinationBucketName)
                .destinationKey(destinationFileName)
                .acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
                .build();
        
        return multipartClient.copyObject(copyRequest)
                .thenApply(response -> {
                    monitor.info(format("Successfully copied S3 object %s/%s to %s/%s.", sourceBucketName, sourceKey, destinationBucketName, destinationFileName));
                    return StreamResult.success();
                })
                .exceptionally(throwable -> {
                    var message = format("Exception during S3 copy operation: %s", throwable.getMessage());
                    monitor.severe(message);
                    return StreamResult.error(message);
                });
    }
    
    @Override
    public CompletableFuture<StreamResult<Object>> transfer(DataFlowStartMessage request, DataSink sink) {
        throw new UnsupportedOperationException("not implemented");
    }
    
    @Override
    public StreamResult<Void> terminate(DataFlow dataFlow) {
        return StreamResult.success();
    }
    
    @Override
    public void closeAll() {
    
    }
    
    private String getDestinationFileName(String key, String folder) {
        if (folder == null) {
            return key;
        }
        
        return folder.endsWith("/") ? folder + key : format("%s/%s", folder, key);
    }
    
    private boolean sameEndpointOverride(String source, String destination) {
        if (source == null && destination == null) {
            return true;
        } else if (source == null || destination == null) {
            return false;
        } else {
            return source.equals(destination);
        }
    }
    
    private SecretToken getCredentials(DataAddress source) {
        try {
            return ofNullable(source.getKeyName())
                    .filter(keyName -> !StringUtils.isNullOrBlank(keyName))
                    .map(vault::resolveSecret)
                    .filter(secret -> !StringUtils.isNullOrBlank(secret))
                    .map(secret -> typeManager.readValue(secret, AwsTemporarySecretToken.class))
                    .orElse(null);
        } catch (EdcException e) {
            // failed to deserialize secret
            return null;
        }
    }
}
