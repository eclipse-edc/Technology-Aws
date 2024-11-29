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

import org.eclipse.edc.boot.system.injection.ObjectFactory;
import org.eclipse.edc.junit.extensions.DependencyInjectionExtension;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.system.configuration.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;

import java.net.URI;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(DependencyInjectionExtension.class)
class AwsSecretsManagerVaultExtensionTest {

    @Test
    void configOptionRegionNotProvided_shouldThrowException(ServiceExtensionContext context) {
        when(context.getMonitor()).thenReturn(mock(Monitor.class));
        var extension = new AwsSecretsManagerVaultExtension();

        Assertions.assertThrows(NullPointerException.class, () -> extension.createVault(context));
    }

    @Test
    void configOptionRegionProvided_shouldNotThrowException(ObjectFactory factory,
            ServiceExtensionContext context) {
        var config = ConfigFactory.fromMap(Map.of(
                "edc.vault.aws.region", "eu-west-1"
        ));
        when(context.getConfig()).thenReturn(config);
        var extension = factory.constructInstance(AwsSecretsManagerVaultExtension.class);

        var vault = extension.createVault(context);

        assertThat(vault).extracting("smClient", type(SecretsManagerClient.class))
                .satisfies(client -> {
                    assertThat(client.serviceClientConfiguration().region()).isEqualTo(
                            Region.of("eu-west-1"));
                });
    }

    @Test
    void configOptionEndpointOverrideProvided_shouldNotThrowException(ObjectFactory factory,
            ServiceExtensionContext context) {
        var config = ConfigFactory.fromMap(Map.of(
                "edc.vault.aws.region", "eu-west-1",
                "edc.vault.aws.endpoint.override", "http://localhost:4566"
        ));
        when(context.getConfig()).thenReturn(config);
        var extension = factory.constructInstance(AwsSecretsManagerVaultExtension.class);

        var vault = extension.createVault(context);

        assertThat(vault).extracting("smClient", type(SecretsManagerClient.class))
                .satisfies(client -> {
                    assertThat(client.serviceClientConfiguration().endpointOverride()).contains(
                            URI.create("http://localhost:4566"));
                });
    }
}
