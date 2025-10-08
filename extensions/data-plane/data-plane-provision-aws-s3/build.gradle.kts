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
 */

plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.dataplane)
    api(project(":extensions:common:aws:aws-s3-core"))

    testImplementation(testFixtures(project(":extensions:common:aws:aws-s3-test")))
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.localstack)
}
