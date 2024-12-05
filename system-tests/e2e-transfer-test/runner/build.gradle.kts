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

plugins {
    `java-library`
}

dependencies {
    testImplementation(project(":spi:common:aws-spi"))
    testImplementation(project(":extensions:common:aws:aws-s3-core"))
    testImplementation(project(":extensions:control-plane:provision:provision-aws-s3-copy"))
    testImplementation(project(":extensions:data-plane:data-plane-aws-s3-copy"))

    testImplementation(libs.edc.junit)
    testImplementation(testFixtures(libs.edc.management.api.test.fixtures))

    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.localstack)
    testImplementation(libs.restAssured)
    testImplementation(libs.awaitility)
}
