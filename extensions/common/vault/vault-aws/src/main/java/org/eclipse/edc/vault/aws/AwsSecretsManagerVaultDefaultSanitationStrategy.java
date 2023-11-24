/*
 *  Copyright (c) 2023 - 2023 Amazon Web Services
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Amazon Web Services - initial implementation
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - Fix hashcode append logic
 *
 */

package org.eclipse.edc.vault.aws;

import org.eclipse.edc.spi.monitor.Monitor;

public class AwsSecretsManagerVaultDefaultSanitationStrategy implements AwsSecretsManagerVaultSanitationStrategy {
    private final Monitor monitor;

    public AwsSecretsManagerVaultDefaultSanitationStrategy(Monitor monitor) {
        this.monitor = monitor;
    }

    /**
     * Many-to-one mapping from all strings into set of strings that only contains valid AWS Secrets Manager key names.
     * The implementation replaces all illegal characters with '_' and attaches the hash code of the original string,
     * when illegal characters are replaced, to minimize the likelihood of key collisions.
     *
     * @param originalKey any key
     * @return Valid AWS Secrets Manager key
     */
    @Override
    public String sanitizeKey(String originalKey) {
        var key = originalKey;
        boolean originalKeyReplaced = false;

        if (originalKey.length() > 512) {
            //Substring to 500, to add the 12 char hashcode
            key = originalKey.substring(0, 500);
            originalKeyReplaced = true;
        }

        var sb = new StringBuilder();
        for (int i = 0; i < key.length(); i++) {
            var c = key.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '/' && c != '_' && c != '+' && c != '.' && c != '@' && c != '-') {
                originalKeyReplaced = true;
                sb.append('-');
            } else {
                sb.append(c);
            }
        }
        
        if (originalKeyReplaced) {
            // Only add the hashcode if the original key has been replaced
            sb.append('_').append(originalKey.hashCode());
            monitor.warning(String.format("AWS Secret Manager vault reduced length or replaced illegal characters " +
                    "in original key name: %s. New name is %s", originalKey, sb.toString()));
        }
        var newKey = sb.toString();
        return newKey;
    }
}