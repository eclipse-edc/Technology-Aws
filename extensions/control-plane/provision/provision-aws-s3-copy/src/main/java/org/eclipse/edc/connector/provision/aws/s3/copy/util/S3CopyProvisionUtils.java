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

package org.eclipse.edc.connector.provision.aws.s3.copy.util;

import org.eclipse.edc.aws.s3.AwsSecretToken;
import org.eclipse.edc.aws.s3.AwsTemporarySecretToken;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.SecretToken;
import org.eclipse.edc.connector.provision.aws.s3.copy.S3CopyResourceDefinition;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.util.string.StringUtils;

import java.io.IOException;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;

/**
 * Provides methods which are required in multiple places during the (de)provisioning for
 * S3-to-S3 copy transfers.
 */
public class S3CopyProvisionUtils {
    
    private S3CopyProvisionUtils() {}
    
    /**
     * Creates the name that will be used for provisioned resources like AWS roles, policies and
     * session names to provide consistent naming of AWS resources that belong to the same EDC
     * transfer process.
     *
     * @param resourceDefinition resource definition
     * @return the name for created resources
     */
    public static String resourceIdentifier(S3CopyResourceDefinition resourceDefinition) {
        return resourceIdentifier(resourceDefinition.getTransferProcessId());
    }
    
    /**
     * Creates the name that will be used for provisioned resources like AWS roles, policies and
     * session names to provide consistent naming of AWS resources that belong to the same EDC
     * transfer process.
     *
     * @param transferProcessId id of the transfer process
     * @return the name for created resources
     */
    public static String resourceIdentifier(String transferProcessId) {
        return format("edc-transfer_%s", transferProcessId);
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
                .orElse(null);
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
