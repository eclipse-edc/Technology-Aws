/*
 *  Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - initial API and implementation
 *
 */

rootProject.name = "technology-aws"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
        maven {
            url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        }
    }
}

include(":extensions:common:aws:aws-s3-test")
include(":extensions:common:aws:aws-s3-core")
include(":extensions:common:s3-copy-lib")
include(":extensions:common:vault:vault-aws")
include(":extensions:common:validator:validator-data-address-s3")

include(":extensions:control-plane:provision:provision-aws-s3")
include(":extensions:control-plane:provision:provision-aws-s3-copy")

include(":extensions:data-plane:data-plane-aws-s3")
include(":extensions:data-plane:data-plane-aws-s3-copy")

include(":spi:common:aws-spi")

include(":system-tests:e2e-transfer-test:runner")
include(":system-tests:e2e-transfer-test:runtime")
