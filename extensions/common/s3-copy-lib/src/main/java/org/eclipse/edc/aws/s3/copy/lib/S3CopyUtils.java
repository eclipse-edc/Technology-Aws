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

package org.eclipse.edc.aws.s3.copy.lib;

import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.SecretToken;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.util.string.StringUtils;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;

/**
 * Provides utility methods needed by both the provisioning and the data plane extension for
 * S3 copy transfers.
 */
public class S3CopyUtils {
    
    private S3CopyUtils() {}
    
    /**
     * Creates the S3 destination file key for a transfer from destination file name and folder.
     *
     * @param key destination file name
     * @param folder destination folder
     * @return the S3 destiation file key
     */
    public static String getDestinationFileName(String key, String folder) {
        if (folder == null) {
            return key;
        }
        
        return folder.endsWith("/") ? folder + key : format("%s/%s", folder, key);
    }
    
    /**
     * Determines whether a transfer with given source and destination address can be executed as
     * an S3 copy transfer. Requires both data addresses to be of the same type and have the same
     * endpoint override, if any.
     *
     * @param source source data address
     * @param destination destination data address
     * @return true, if transfer can be executed as S3 copy; false otherwise
     */
    public static boolean applicableForS3CopyTransfer(DataAddress source, DataAddress destination) {
        if (destination == null) {
            return false;
        }
        
        var sourceType = source.getType();
        var sinkType = destination.getType();
        var sourceEndpointOverride = source.getStringProperty(ENDPOINT_OVERRIDE);
        var destinationEndpointOverride = destination.getStringProperty(ENDPOINT_OVERRIDE);
        
        // only applicable for S3-to-S3 transfer
        var isSameType = S3BucketSchema.TYPE.equals(sourceType) && S3BucketSchema.TYPE.equals(sinkType);
        
        // if endpointOverride set, it needs to be the same for both source & destination
        var hasSameEndpointOverride = sameEndpointOverride(sourceEndpointOverride, destinationEndpointOverride);
        
        return isSameType && hasSameEndpointOverride;
    }
    
    private static boolean sameEndpointOverride(String source, String destination) {
        if (source == null && destination == null) {
            return true;
        } else if (source == null || destination == null) {
            return false;
        } else {
            return source.equals(destination);
        }
    }
    
    /**
     * Reads a secret from the vault and deserializes it to an {@link AwsSecretToken} or
     * {@link AwsTemporarySecretToken} depending on its content.
     *
     * @param secretKeyName name of the secret
     * @param vault vault from which to read the secret
     * @param typeManager type manager required for deserialization
     * @return the deserialized secret token
     */
    public static SecretToken getSecretTokenFromVault(String secretKeyName, Vault vault, TypeManager typeManager) {
        return ofNullable(secretKeyName)
                .filter(keyName -> !StringUtils.isNullOrBlank(keyName))
                .map(vault::resolveSecret)
                .filter(secret -> !StringUtils.isNullOrBlank(secret))
                .map(secret -> deserializeSecretToken(secret, typeManager))
                .orElseThrow(() -> new EdcException(format("Failed to resolve or parse secret with key '%s'", secretKeyName)));
    }
    
    private static SecretToken deserializeSecretToken(String secret, TypeManager typeManager) {
        try {
            var objectMapper = typeManager.getMapper();
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
