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

import static java.lang.String.format;

class S3DataSink extends ParallelSink {

    private S3Client client;
    private String bucketName;
    private String keyName;
    private String folderName;
    private int chunkSize;

    private S3DataSink() {
    }

    @Override
    protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {

        for (var part : parts) {

            var key = getDestinationObjectName(part.name());

            try (var input = part.openStream()) {

                var partNumber = 1;
                var completedParts = new ArrayList<CompletedPart>();

                var uploadId = client.createMultipartUpload(CreateMultipartUploadRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build()).uploadId();

                var bytesChunk = input.readNBytes(chunkSize);

                while (bytesChunk.length > 0) {

                    completedParts.add(CompletedPart.builder().partNumber(partNumber)
                            .eTag(client.uploadPart(UploadPartRequest.builder()
                                    .bucket(bucketName)
                                    .key(key)
                                    .uploadId(uploadId)
                                    .partNumber(partNumber)
                                    .build(), RequestBody.fromByteBuffer(ByteBuffer.wrap(bytesChunk))).eTag()).build());

                    bytesChunk = input.readNBytes(chunkSize);

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

            } catch (S3DataSourceException e) {
                return downloadFailure(e, key);
            } catch (Exception e) {
                return uploadFailure(e, key);
            }
        }

        return StreamResult.success();
    }

    private String getDestinationObjectName(String partName) {
        if (!StringUtils.isNullOrEmpty(folderName)) {
            return folderName.endsWith("/") ? folderName + partName : folderName + "/" + partName;
        }
        return partName;
    }

    @NotNull
    private StreamResult<Object> downloadFailure(Exception e, String keyName) {
        var message = format("Error downloading the %s object: %s", keyName, e.getMessage());
        monitor.severe(message, e);
        return StreamResult.error(message);
    }

    @NotNull
    private StreamResult<Object> uploadFailure(Exception e, String keyName) {
        var message = format("Error uploading the %s object: %s", keyName, e.getMessage());
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
