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

plugins {
    `java-library`
    id("application")
    alias(libs.plugins.shadow)
}

dependencies {
    implementation(project(":extensions:common:aws:aws-s3-core"))
    implementation(project(":extensions:common:validator:validator-data-address-s3"))
    implementation(project(":extensions:data-plane:data-plane-aws-s3"))

    implementation(libs.edc.bom.controlplane.base)
    implementation(libs.edc.control.plane.api.client)
    implementation(libs.edc.iam.mock)
    implementation(libs.edc.api.secrets)

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
