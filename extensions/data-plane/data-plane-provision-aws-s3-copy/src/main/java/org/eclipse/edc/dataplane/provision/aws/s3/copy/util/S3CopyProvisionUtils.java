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

package org.eclipse.edc.dataplane.provision.aws.s3.copy.util;

import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;

import static java.lang.String.format;

/**
 * Provides methods which are required in multiple places during the (de)provisioning for
 * S3-to-S3 copy transfers.
 */
public class S3CopyProvisionUtils {
    
    private S3CopyProvisionUtils() {}

    /**
     * Creates the name that will be used for provisioned resources like AWS roles, policies and
     * session names to provide consistent naming of AWS resources that belong to the same EDC
     * data flow.
     *
     * @param resourceDefinition resource definition
     * @return the name for created resources
     */
    public static String resourceIdentifier(ProvisionResource resourceDefinition) {
        return resourceIdentifier(resourceDefinition.getFlowId());
    }
    
    /**
     * Creates the name that will be used for provisioned resources like AWS roles, policies and
     * session names to provide consistent naming of AWS resources that belong to the same EDC
     * transfer process.
     *
     * @param dataFlowId id of the data flow
     * @return the name for created resources
     */
    public static String resourceIdentifier(String dataFlowId) {
        return format("edc-transfer_%s", dataFlowId);
    }
}
