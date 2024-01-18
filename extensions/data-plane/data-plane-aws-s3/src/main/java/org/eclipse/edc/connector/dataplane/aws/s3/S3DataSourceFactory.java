/*
 *  Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
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
import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.S3BucketSchema;
import org.eclipse.edc.aws.s3.S3ClientRequest;
import org.eclipse.edc.aws.s3.validation.S3DataAddressCredentialsValidator;
import org.eclipse.edc.aws.s3.validation.S3DataAddressValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.eclipse.edc.util.string.StringUtils;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3Client;

import static org.eclipse.edc.aws.s3.S3BucketSchema.ACCESS_KEY_ID;
import static org.eclipse.edc.aws.s3.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.S3BucketSchema.KEY_PREFIX;
import static org.eclipse.edc.aws.s3.S3BucketSchema.REGION;
import static org.eclipse.edc.aws.s3.S3BucketSchema.SECRET_ACCESS_KEY;

public class S3DataSourceFactory implements DataSourceFactory {

    private final Validator<DataAddress> validation = new S3DataAddressValidator();
    private final Validator<DataAddress> credentialsValidation = new S3DataAddressCredentialsValidator();
    private final AwsClientProvider clientProvider;
    private final Vault vault;
    private final TypeManager typeManager;

    public S3DataSourceFactory(AwsClientProvider clientProvider, Vault vault, TypeManager typeManager) {
        this.clientProvider = clientProvider;
        this.vault = vault;
        this.typeManager = typeManager;
    }

    @Override
    public boolean canHandle(DataFlowRequest request) {
        return S3BucketSchema.TYPE.equals(request.getSourceDataAddress().getType());
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var source = request.getSourceDataAddress();

        return S3DataSource.Builder.newInstance()
                .bucketName(source.getStringProperty(BUCKET_NAME))
                .keyName(source.getKeyName())
                .keyPrefix(source.getStringProperty(KEY_PREFIX))
                .client(getS3Client(source))
                .build();
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        var source = request.getSourceDataAddress();

        return validation.validate(source).flatMap(ValidationResult::toResult);
    }

    private S3Client getS3Client(DataAddress address) {

        String endpointOverride = address.getStringProperty(ENDPOINT_OVERRIDE);

        S3Client client;
        var secret = StringUtils.isNullOrBlank(address.getKeyName()) ? null : vault.resolveSecret(address.getKeyName());
        if (secret != null) {
            var secretToken = typeManager.readValue(secret, AwsSecretToken.class);
            client = clientProvider.s3Client(S3ClientRequest.from(address.getStringProperty(REGION), endpointOverride, secretToken));
        } else if (credentialsValidation.validate(address).succeeded()) {
            var secretToken = new AwsSecretToken(address.getStringProperty(ACCESS_KEY_ID), address.getStringProperty(SECRET_ACCESS_KEY));
            client = clientProvider.s3Client(S3ClientRequest.from(address.getStringProperty(REGION), endpointOverride, secretToken));
        } else {
            client = clientProvider.s3Client(S3ClientRequest.from(address.getStringProperty(REGION), endpointOverride));
        }
        return client;
    }

}
