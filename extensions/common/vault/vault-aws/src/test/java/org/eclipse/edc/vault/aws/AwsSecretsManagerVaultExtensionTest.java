/*
 *  Copyright (c) 2023 Amazon Web Services
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Amazon Web Services - Initial implementation
 *
 */

package org.eclipse.edc.vault.aws;

import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsSecretsManagerVaultExtensionTest {
    private static ServiceExtensionContext context;

    @BeforeAll
    public static void beforeAll() {
        context = mock(ServiceExtensionContext.class);
        when(context.getMonitor()).thenReturn(mock(Monitor.class));
    }


    @Test
    void configOptionRegionNotProvided_shouldThrowException() {
        var extension = new AwsSecretsManagerVaultExtension();

        Assertions.assertThrows(NullPointerException.class, () -> extension.createVault(context));
    }

    @Test
    void configOptionRegionProvided_shouldNotThrowException() {
        var extension = new AwsSecretsManagerVaultExtension();
        extension.vaultRegion = "eu-west-1";

        var vault = extension.createVault(context);

        assertThat(vault).extracting("smClient", type(SecretsManagerClient.class)).satisfies(client -> {
            assertThat(client.serviceClientConfiguration().region()).isEqualTo(Region.of("eu-west-1"));
        });
    }

    @Test
    void configOptionEndpointOverrideProvided_shouldNotThrowException() {
        var extension = new AwsSecretsManagerVaultExtension();
        extension.vaultRegion = "eu-west-1";
        extension.vaultAwsEndpointOverride = "http://localhost:4566";

        var vault = extension.createVault(context);

        assertThat(vault).extracting("smClient", type(SecretsManagerClient.class)).satisfies(client -> {
            assertThat(client.serviceClientConfiguration().endpointOverride()).contains(URI.create("http://localhost:4566"));
        });
    }
}
