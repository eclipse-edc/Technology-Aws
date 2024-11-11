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
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;

public class AwsS3CopyTransferService implements TransferService {
    
    private final AwsClientProvider clientProvider;
    private final Monitor monitor;
    private final Vault vault;
    private final TypeManager typeManager;
    private final DataAddressValidatorRegistry validator;
    
    public AwsS3CopyTransferService(AwsClientProvider clientProvider, Monitor monitor, Vault vault, TypeManager typeManager, DataAddressValidatorRegistry validator) {
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.vault = vault;
        this.typeManager = typeManager;
        this.validator = validator;
    }
    
    //TODO if used with standard S3 data plane in one runtime, the pipeline service would also be applicable for this transfer
    @Override
    public boolean canHandle(DataFlowStartMessage request) {
        return S3BucketSchema.TYPE.equals(request.getSourceDataAddress().getType()) &&
                S3BucketSchema.TYPE.equals(request.getDestinationDataAddress().getType());
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
        
        var violation = Violation.violation("No credential found in vault for given key.", "keyName", source.getKeyName());
        return ValidationResult.failure(violation);
    }
    
    //TODO for now only single file, consider multiple prefixed files
    @Override
    public CompletableFuture<StreamResult<Object>> transfer(DataFlowStartMessage request) {
        var source = request.getSourceDataAddress();
        var sourceRegion = source.getStringProperty(REGION);
        var sourceBucketName = source.getStringProperty(BUCKET_NAME);
        var sourceKey = source.getStringProperty(OBJECT_NAME);
    
        var destination = request.getDestinationDataAddress();
        var destinationBucketName = destination.getStringProperty(BUCKET_NAME);
        var destinationKey = destination.getStringProperty(OBJECT_NAME);
        
        var secretToken = getCredentials(source);
        if (secretToken == null) {
            throw new EdcException("Missing credentials.");
        }
        var s3ClientRequest = S3ClientRequest.from(sourceRegion, null, secretToken);
        var s3Client = clientProvider.s3AsyncClient(s3ClientRequest);
    
        var copyRequest = CopyObjectRequest.builder()
                .sourceBucket(sourceBucketName)
                .sourceKey(sourceKey)
                .destinationBucket(destinationBucketName)
                .destinationKey(destinationKey)
                .build();
        
        return s3Client.copyObject(copyRequest)
                .thenApply(response -> {
                    monitor.info(format("Successfully copied S3 object %s/%s to %s/%s.", sourceBucketName, sourceKey, destinationBucketName, destinationKey));
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
    
    private SecretToken getCredentials(DataAddress source) {
        return ofNullable(source.getKeyName())
                .filter(keyName -> !StringUtils.isNullOrBlank(keyName))
                .map(vault::resolveSecret)
                .filter(secret -> !StringUtils.isNullOrBlank(secret))
                .map(secret -> typeManager.readValue(secret, AwsTemporarySecretToken.class))
                .orElse(null);
    }
}
