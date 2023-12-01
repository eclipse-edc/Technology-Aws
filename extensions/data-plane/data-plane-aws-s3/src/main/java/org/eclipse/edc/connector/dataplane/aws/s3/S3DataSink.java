/*
 *  Copyright (c) 2022 - 2023 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
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

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
import org.eclipse.edc.util.string.StringUtils;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

class S3DataSink extends ParallelSink {

    private S3Client client;
    private String bucketName;
    private String keyName;
    private String keyPrefix;
    private int chunkSize;

    public static final String COMPLETE_BLOB_NAME = ".complete";

    private final List<String> completedFiles = new ArrayList<>();

    private S3DataSink() {
    }

    @Override
    protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {
        for (var part : parts) {
            var key = StringUtils.isNullOrBlank(keyPrefix) ? keyName : part.name();
            try (var input = part.openStream()) {
                var partNumber = 1;
                var completedParts = new ArrayList<CompletedPart>();

                var uploadId = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build()).uploadId();

                while (true) {
                    var bytesChunk = input.readNBytes(chunkSize);

                    if (bytesChunk.length < 1) {
                        break;
                    }

                    completedParts.add(CompletedPart.builder().partNumber(partNumber)
                            .eTag(client.uploadPart(UploadPartRequest.builder()
                                    .bucket(bucketName)
                                    .key(key)
                                    .uploadId(uploadId)
                                    .partNumber(partNumber)
                                    .build(), RequestBody.fromByteBuffer(ByteBuffer.wrap(bytesChunk))).eTag()).build());
                    partNumber++;
                }

                client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build());

            } catch (Exception e) {
                return uploadFailure(e, key);
            }
            registerCompletedFile(key);
        }

        return StreamResult.success();
    }

    void registerCompletedFile(String name) {
        completedFiles.add(name + COMPLETE_BLOB_NAME);
    }

    @Override
    protected StreamResult<Object> complete() {
        for (var completedFile : completedFiles) {
            var request = PutObjectRequest.builder().bucket(bucketName).key(completedFile).build();
            try {
                client.putObject(request, RequestBody.empty());

            } catch (Exception e) {
                return uploadFailure(e, completedFile);
            }
        }
        return super.complete();

    }

    @NotNull
    private StreamResult<Object> uploadFailure(Exception e, String keyName) {
        var message = format("Error writing the %s object on the %s bucket: %s", keyName, bucketName, e.getMessage());
        monitor.severe(message, e);
        return StreamResult.error(message);
    }

    public static class Builder extends ParallelSink.Builder<Builder, S3DataSink> {

        private Builder() {
            super(new S3DataSink());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder client(S3Client client) {
            sink.client = client;
            return this;
        }

        public Builder bucketName(String bucketName) {
            sink.bucketName = bucketName;
            return this;
        }

        public Builder keyName(String keyName) {
            sink.keyName = keyName;
            return this;
        }

        public Builder keyPrefix(String keyPrefix) {
            sink.keyPrefix = keyPrefix;
            return this;
        }

        public Builder chunkSizeBytes(int chunkSize) {
            sink.chunkSize = chunkSize;
            return this;
        }

        @Override
        protected void validate() {}
    }
}
