/*
 *  Copyright (c) 2022 - 2024 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
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

package org.eclipse.edc.connector.dataplane.aws.s3;

import org.eclipse.edc.connector.dataplane.aws.s3.exceptions.S3DataSourceException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure.Reason.GENERAL_ERROR;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.failure;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;

class S3DataSource implements DataSource {

    private String bucketName;
    private String keyName;
    private String keyPrefix;
    private S3Client client;

    private S3DataSource() {
    }

    @Override
    public StreamResult<Stream<Part>> openPartStream() {

        if (keyPrefix != null) {

            var s3Objects = this.fetchPrefixedS3Objects();

            if (s3Objects.isEmpty()) {
                return failure(new StreamFailure(List.of("Error listing S3 objects in the bucket: Object not found"), GENERAL_ERROR));
            }

            var s3PartStream = s3Objects.stream()
                    .map(S3Object::key)
                    .map(key -> (Part) new S3Part(client, key, bucketName));

            return success(s3PartStream);

        }

        return success(Stream.of(new S3Part(client, keyName, bucketName)));
    }

    /**
     * Fetches S3 objects with the specified prefix.
     *
     * @return A list of S3 objects.
     */
    private List<S3Object> fetchPrefixedS3Objects() {

        String continuationToken = null;
        List<S3Object> s3Objects = new ArrayList<>();

        do {

            var listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(keyPrefix)
                    .continuationToken(continuationToken)
                    .build();

            var response = client.listObjectsV2(listObjectsRequest);

            s3Objects.addAll(response.contents());

            continuationToken = response.nextContinuationToken();

        } while (continuationToken != null);

        return s3Objects;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    private static class S3Part implements Part {
        private final S3Client client;
        private final String keyName;
        private final String bucketName;

        S3Part(S3Client client, String keyName, String bucketName) {
            this.client = client;
            this.keyName = keyName;
            this.bucketName = bucketName;
        }

        @Override
        public String name() {
            return keyName;
        }

        @Override
        public long size() {
            var request = HeadObjectRequest.builder().key(keyName).bucket(bucketName).build();
            return client.headObject(request).contentLength();
        }

        @Override
        public InputStream openStream() {
            try {
                var request = GetObjectRequest.builder().key(keyName).bucket(bucketName).build();
                return client.getObject(request);
            } catch (Exception e) {
                throw new S3DataSourceException(e.getMessage(), e);
            }
        }
    }

    public static class Builder {
        private final S3DataSource source;

        private Builder() {
            source = new S3DataSource();
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder bucketName(String bucketName) {
            source.bucketName = bucketName;
            return this;
        }

        public Builder keyName(String keyName) {
            source.keyName = keyName;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            source.keyPrefix = keyPrefix;
            return this;
        }

        public Builder client(S3Client client) {
            source.client = client;
            return this;
        }

        public S3DataSource build() {
            return source;
        }
    }
}
