/*
 *  Copyright (c) 2022 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 */


plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.dataplane)
    implementation(libs.edc.lib.util)
    implementation(libs.edc.core.dataPlane.util)
    implementation(project(":extensions:common:aws:aws-s3-core"))

    implementation(libs.failsafe.core)

    testImplementation(libs.edc.core.dataplane)
    testImplementation(testFixtures(project(":extensions:common:aws:aws-s3-test")))
    testImplementation(libs.edc.junit)
    testImplementation(libs.testcontainers.junit.jupiter)
}


