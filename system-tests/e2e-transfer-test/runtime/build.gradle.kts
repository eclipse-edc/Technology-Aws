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
    testImplementation(project(":extensions:data-plane:data-plane-transfer-service-selection"))

    implementation(libs.edc.control.api.configuration)
    implementation(libs.edc.control.plane.api.client)
    implementation(libs.edc.control.plane.api)
    implementation(libs.edc.core.controlplane)
    implementation(libs.edc.token.core)
    implementation(libs.edc.dsp)
    implementation(libs.edc.ext.http)
    implementation(libs.edc.config.filesystem)
    implementation(libs.edc.iam.mock)
    implementation(libs.edc.api.management)
    implementation(libs.edc.api.secrets)
    implementation(libs.edc.transfer.data.plane.signaling)

    implementation(libs.edc.dpf.selector.api)
    implementation(libs.edc.dpf.selector.core)

    implementation(libs.edc.data.plane.self.registration)
    implementation(libs.edc.data.plane.signaling.api)
    implementation(libs.edc.data.plane.public.api)
    implementation(libs.edc.core.dataplane)
    implementation(libs.edc.data.plane.iam)
}
