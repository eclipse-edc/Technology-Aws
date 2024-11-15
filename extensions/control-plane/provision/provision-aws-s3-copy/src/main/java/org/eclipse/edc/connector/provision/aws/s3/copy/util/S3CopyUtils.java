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

public class S3CopyUtils {
    
    private S3CopyUtils() {}
    
    public static String roleIdentifier(S3CopyResourceDefinition resourceDefinition) {
        return format("edc-transfer_%s", resourceDefinition.getTransferProcessId());
    }
    
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
