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
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class S3DataSink extends ParallelSink {

    private S3Client client;
    private String bucketName;
    @Deprecated(since = "0.5.2")
    private String keyName;
    private String objectName;
    private String folderName;
    private int chunkSize;

    private S3DataSink() {
    }

    @Override
    protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {
        for (var part : parts) {
            var key = getDestinationObjectName(part.name(), parts.size());
            try (var input = part.openStream()) {

                var completedParts = new ArrayList<CompletedPart>();

                var uploadId = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build()).uploadId();

                var partNumber = 1;
                byte[] bytesChunk = input.readNBytes(chunkSize);
                do {
                    completedParts.add(CompletedPart.builder().partNumber(partNumber)
                            .eTag(client.uploadPart(UploadPartRequest.builder()
                                    .bucket(bucketName)
                                    .key(key)
                                    .uploadId(uploadId)
                                    .partNumber(partNumber)
                                    .build(), RequestBody.fromByteBuffer(ByteBuffer.wrap(bytesChunk))).eTag()).build());
                    bytesChunk = input.readNBytes(chunkSize);
                    partNumber++;
                } while (bytesChunk.length > 0);

                client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder()
                                .parts(completedParts)
                                .build())
                        .build());

            } catch (S3DataSourceException e) {
                return transferFailure(e, "download", key);
            } catch (Exception e) {
                return transferFailure(e, "upload", key);
            }
        }

        return StreamResult.success();
    }

    private String getDestinationObjectName(String partName, int partsSize) {
        var name = useObjectName(partsSize) ? objectName : partName;
        if (!StringUtils.isNullOrEmpty(folderName)) {
            return folderName.endsWith("/") ? folderName + name : folderName + "/" + name;
        }

        return name;
    }

    private boolean useObjectName(int partsSize) {
        return partsSize == 1 && !StringUtils.isNullOrEmpty(objectName);
    }

    @NotNull
    private StreamResult<Object> transferFailure(Exception e, String operation, String objectKeyName) {
        var message = "Failed to %s the %s object: %s".formatted(operation, objectKeyName, e.getMessage());
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

        public Builder objectName(String objectName) {
            sink.objectName = objectName;
            return this;
        }

        public Builder folderName(String folderName) {
            sink.folderName = folderName;
            return this;
        }

        public Builder chunkSizeBytes(int chunkSize) {
            sink.chunkSize = chunkSize;
            return this;
        }

        @Override
        protected void validate() {
        }
    }
}
