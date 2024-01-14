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

import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AwsSecretsManagerVaultExtensionTest {

    private final Monitor monitor = mock(Monitor.class);
    private final AwsSecretsManagerVaultExtension extension = new AwsSecretsManagerVaultExtension();

    @Test
    void configOptionRegionNotProvided_shouldThrowException() {
        ServiceExtensionContext invalidContext = mock(ServiceExtensionContext.class);
        when(invalidContext.getMonitor()).thenReturn(monitor);

        Assertions.assertThrows(EdcException.class, () -> extension.createVault(invalidContext));
    }

    @Test
    void configOptionRegionProvided_shouldNotThrowException() {
        ServiceExtensionContext validContext = mock(ServiceExtensionContext.class);
        when(validContext.getSetting("edc.vault.aws.region", null)).thenReturn("eu-west-1");
        when(validContext.getMonitor()).thenReturn(monitor);

        extension.createVault(validContext);
    }

}
