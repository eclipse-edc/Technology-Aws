/*
 *  Copyright (c) 2022 - 2023 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
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
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3BucketSchema;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.validation.S3DataAddressCredentialsValidator;
import org.eclipse.edc.aws.s3.validation.S3DestinationDataAddressValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.util.string.StringUtils;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.ExecutorService;

import static java.util.Optional.ofNullable;
import static org.eclipse.edc.aws.s3.S3BucketSchema.ACCESS_KEY_ID;
import static org.eclipse.edc.aws.s3.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.S3BucketSchema.FOLDER_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.REGION;
import static org.eclipse.edc.aws.s3.S3BucketSchema.SECRET_ACCESS_KEY;


public class S3DataSinkFactory implements DataSinkFactory {
    private final Validator<DataAddress> validation = new S3DestinationDataAddressValidator();
    private final Validator<DataAddress> credentialsValidation = new S3DataAddressCredentialsValidator();
    private final AwsClientProvider clientProvider;
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final Vault vault;
    private final TypeManager typeManager;
    private final int chunkSizeInBytes;

    public S3DataSinkFactory(AwsClientProvider clientProvider, ExecutorService executorService, Monitor monitor, Vault vault, TypeManager typeManager, int chunkSizeInBytes) {
        this.clientProvider = clientProvider;
        this.executorService = executorService;
        this.monitor = monitor;
        this.vault = vault;
        this.typeManager = typeManager;
        this.chunkSizeInBytes = chunkSizeInBytes;
    }

    @Override
    public boolean canHandle(DataFlowStartMessage request) {
        return S3BucketSchema.TYPE.equals(request.getDestinationDataAddress().getType());
    }

    @Override
    public DataSink createSink(DataFlowStartMessage request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var destination = request.getDestinationDataAddress();

        return S3DataSink.Builder.newInstance()
                .bucketName(destination.getStringProperty(BUCKET_NAME))
                .keyName(destination.getKeyName())
                .objectName(destination.getStringProperty(OBJECT_NAME))
                .folderName(destination.getStringProperty(FOLDER_NAME))
                .requestId(request.getId())
                .executorService(executorService)
                .monitor(monitor)
                .client(createS3Client(destination))
                .chunkSizeBytes(chunkSizeInBytes)
                .build();
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage request) {
        var destination = request.getDestinationDataAddress();

        return validation.validate(destination).flatMap(ValidationResult::toResult);
    }

    private S3Client createS3Client(DataAddress address) {
        var endpointOverride = address.getStringProperty(ENDPOINT_OVERRIDE);
        var region = address.getStringProperty(REGION);

        var awsSecretToken = ofNullable(address.getKeyName())
                .filter(keyName -> !StringUtils.isNullOrBlank(keyName))
                .map(vault::resolveSecret)
                .filter(secret -> !StringUtils.isNullOrBlank(secret))
                .map(s -> typeManager.readValue(s, AwsTemporarySecretToken.class));

        if (awsSecretToken.isPresent()) {
            return clientProvider.s3Client(S3ClientRequest.from(region, endpointOverride, awsSecretToken.get()));
        } else if (credentialsValidation.validate(address).succeeded()) {
            var accessKeyId = address.getStringProperty(ACCESS_KEY_ID);
            var secretAccessKey = address.getStringProperty(SECRET_ACCESS_KEY);
            return clientProvider.s3Client(S3ClientRequest.from(region, endpointOverride,
                    new AwsSecretToken(accessKeyId, secretAccessKey)));
        } else {
            return clientProvider.s3Client(S3ClientRequest.from(region, endpointOverride));
        }
    }
}
