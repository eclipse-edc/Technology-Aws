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
 *       Cofinity-X - fix secret deserialization
 *
 */

package org.eclipse.edc.connector.dataplane.aws.s3;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.SecretToken;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.aws.s3.validation.S3DataAddressCredentialsValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.participantcontext.spi.service.ParticipantContextSupplier;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static java.util.Optional.ofNullable;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ACCESS_KEY_ID;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.FOLDER_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.SECRET_ACCESS_KEY;


public class S3DataSinkFactory implements DataSinkFactory {
    private final Validator<DataAddress> credentialsValidation = new S3DataAddressCredentialsValidator();
    private final AwsClientProvider clientProvider;
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final Vault vault;
    private final ObjectMapper objectMapper;
    private final int chunkSizeInBytes;
    private final DataAddressValidatorRegistry dataAddressValidator;
    private final ParticipantContextSupplier singleParticipantContextSupplier;

    public S3DataSinkFactory(AwsClientProvider clientProvider, ExecutorService executorService, Monitor monitor, Vault vault,
                             ObjectMapper objectMapper, int chunkSizeInBytes, DataAddressValidatorRegistry dataAddressValidator,
                             ParticipantContextSupplier singleParticipantContextSupplier) {
        this.clientProvider = clientProvider;
        this.executorService = executorService;
        this.monitor = monitor;
        this.vault = vault;
        this.objectMapper = objectMapper;
        this.chunkSizeInBytes = chunkSizeInBytes;
        this.dataAddressValidator = dataAddressValidator;
        this.singleParticipantContextSupplier = singleParticipantContextSupplier;
    }

    @Override
    public String supportedType() {
        return S3BucketSchema.TYPE;
    }

    @Override
    public DataSink createSink(DataFlowStartMessage request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var destination = request.getDestinationDataAddress();
        var s3ClientRequest = createS3ClientRequest(destination);

        return S3DataSink.Builder.newInstance()
                .bucketName(destination.getStringProperty(BUCKET_NAME))
                .objectName(destination.getStringProperty(OBJECT_NAME))
                .folderName(destination.getStringProperty(FOLDER_NAME))
                .requestId(request.getId())
                .executorService(executorService)
                .monitor(monitor)
                .client(this.clientProvider.s3Client(s3ClientRequest))
                .chunkSizeBytes(chunkSizeInBytes)
                .build();
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage request) {
        var destination = request.getDestinationDataAddress();

        return dataAddressValidator.validateDestination(destination).flatMap(ValidationResult::toResult);
    }

    private S3ClientRequest createS3ClientRequest(DataAddress address) {
        var endpointOverride = address.getStringProperty(ENDPOINT_OVERRIDE);
        var region = address.getStringProperty(REGION);
        var awsSecretToken = ofNullable(getSecret(address))
                .map(this::deserializeSecretToken);

        if (awsSecretToken.isPresent()) {
            return S3ClientRequest.from(region, endpointOverride, awsSecretToken.get());
        } else if (credentialsValidation.validate(address).succeeded()) {
            var accessKeyId = address.getStringProperty(ACCESS_KEY_ID);
            var secretAccessKey = address.getStringProperty(SECRET_ACCESS_KEY);
            return S3ClientRequest.from(region, endpointOverride, new AwsSecretToken(accessKeyId, secretAccessKey));
        } else {
            return S3ClientRequest.from(region, endpointOverride);
        }
    }

    private @Nullable String getSecret(DataAddress dataAddress) {
        var addressSecret = dataAddress.getStringProperty(DataAddress.EDC_DATA_ADDRESS_SECRET);
        if (addressSecret != null) {
            return addressSecret;
        }

        var keyName = dataAddress.getKeyName();
        if (keyName == null) {
            return null;
        }

        var participantContext = singleParticipantContextSupplier.get()
                .orElseThrow(f -> new EdcException("Failed to obtain participant context for data sink creation"));

        return vault.resolveSecret(participantContext.getParticipantContextId(), keyName);
    }

    private SecretToken deserializeSecretToken(String secret) {
        try {
            var tree = objectMapper.readTree(secret);
            if (tree.has("sessionToken")) {
                return objectMapper.treeToValue(tree, AwsTemporarySecretToken.class);
            } else {
                return objectMapper.treeToValue(tree, AwsSecretToken.class);
            }
        } catch (IOException e) {
            throw new EdcException(e);
        }
    }
}
