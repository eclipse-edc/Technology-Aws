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
    id("application")
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":spi:common:aws-spi"))
    implementation(project(":extensions:common:aws:aws-s3-core"))
    implementation(project(":extensions:control-plane:provision:provision-aws-s3-copy"))
    implementation(project(":extensions:data-plane:data-plane-aws-s3-copy"))
    implementation(project(":extensions:data-plane:data-plane-aws-s3"))
    implementation(project(":extensions:data-plane:data-plane-transfer-service-selection"))

    implementation(libs.edc.boot)
    implementation(libs.edc.control.api.configuration)
    implementation(libs.edc.control.plane.api.client)
    implementation(libs.edc.control.plane.api)
    implementation(libs.edc.core.controlplane)
    implementation(libs.edc.core.connector)
    implementation(libs.edc.token.core)
    implementation(libs.edc.dsp)
    implementation(libs.edc.ext.http)
    implementation(libs.edc.config.filesystem)
    implementation(libs.edc.iam.mock)
    implementation(libs.edc.api.management)
    implementation(libs.edc.api.secrets)
    implementation(libs.edc.transfer.data.plane.signaling)

    implementation(libs.edc.edr.cache.api)
    implementation(libs.edc.edr.store.core)
    implementation(libs.edc.edr.store.receiver)

    implementation(libs.edc.dpf.selector.api)
    implementation(libs.edc.dpf.selector.core)

    implementation(libs.edc.data.plane.self.registration)
    implementation(libs.edc.data.plane.signaling.api)
    implementation(libs.edc.data.plane.public.api)
    implementation(libs.edc.core.dataplane)
    implementation(libs.edc.data.plane.iam)
}

application {
    mainClass.set("org.eclipse.edc.boot.system.runtime.BaseRuntime")
}

var distTar = tasks.getByName("distTar")
var distZip = tasks.getByName("distZip")

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    mergeServiceFiles()
    archiveFileName.set("runtime.jar")
    dependsOn(distTar, distZip)
}
