/*
 *  Copyright (c) 2024 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
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

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;

public class MinioContainer extends GenericContainer<MinioContainer> {

    public MinioContainer() {
        super(DockerImageName.parse("quay.io/minio/minio"));
        setExposedPorts(List.of(9000));
        setEnv(List.of("MINIO_ROOT_USER=root", "MINIO_ROOT_PASSWORD=password"));
        withCommand("server /data");
    }
}
