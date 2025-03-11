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

import org.eclipse.edc.connector.provision.aws.s3.copy.S3CopyResourceDefinition;

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
}
