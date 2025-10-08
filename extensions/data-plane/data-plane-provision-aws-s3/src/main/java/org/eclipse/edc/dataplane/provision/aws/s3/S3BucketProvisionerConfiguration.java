/*
 *  Copyright (c) 2025 Think-it GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Think-it GmbH - initial API and implementation
 *
 */

package org.eclipse.edc.dataplane.provision.aws.s3;

import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.runtime.metamodel.annotation.Settings;

@Settings
public record S3BucketProvisionerConfiguration(
        @Setting(
                key = "edc.dataplane.provision.aws.s3.retries.max",
                description = "Max number of retries in case of failure",
                defaultValue = DEFAULT_MAX_RETRIES + "")
        int maxRetries,
        @Setting(
                key = "edc.dataplane.provision.role.duration.session.max",
                description = "The maximum session duration (in seconds) for the temporary role",
                defaultValue = DEFAULT_MAX_ROLE_SESSION_DURATION + "")
        int roleMaxSessionDuration
) {
    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final int DEFAULT_MAX_ROLE_SESSION_DURATION = 3600;
}
