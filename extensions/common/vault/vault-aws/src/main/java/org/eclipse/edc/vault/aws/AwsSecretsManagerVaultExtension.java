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
 *
 */

package org.eclipse.edc.vault.aws;

import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.net.URI;
import java.util.Optional;

/**
 * This extension registers an implementation of the Vault interface for AWS Secrets Manager.
 * It also registers a VaultPrivateKeyResolver and VaultCertificateResolver, which store and retrieve certificates
 * using the AWS Secretes Manager Vault implementation.
 * The extension requires the "edc.vault.aws.region" parameter to be set to the AWS region in which secrets should be stored.
 */
@Extension(value = org.eclipse.edc.vault.aws.AwsSecretsManagerVaultExtension.NAME)
public class AwsSecretsManagerVaultExtension implements ServiceExtension {
    public static final String NAME = "AWS Secrets Manager Vault";

    @Setting
    private static final String VAULT_AWS_REGION = "edc.vault.aws.region";

    @Setting
    private static final String AWS_ENDPOINT_OVERRIDE = "edc.aws.endpoint.override";

    @Override
    public String name() {
        return NAME;
    }

    @Provider
    public Vault createVault(ServiceExtensionContext context) {
        var vaultRegion = context.getConfig().getString(VAULT_AWS_REGION);
        var vaultEndpointOverride = Optional.of(AWS_ENDPOINT_OVERRIDE)
                .map(key -> context.getSetting(key, null))
                .map(URI::create)
                .orElse(null);

        var smClient = buildSmClient(vaultRegion, vaultEndpointOverride);

        return new AwsSecretsManagerVault(smClient, context.getMonitor(),
                new AwsSecretsManagerVaultDefaultSanitationStrategy(context.getMonitor()));
    }

    private SecretsManagerClient buildSmClient(String vaultRegion, URI vaultEndpointOverride) {
        var builder = SecretsManagerClient.builder()
                .region(Region.of(vaultRegion))
                .endpointOverride(vaultEndpointOverride);
        return builder.build();
    }
}
