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

package org.eclipse.edc.dataplane.provision.aws.s3.copy;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.ENDPOINT_OVERRIDE;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.FOLDER_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.OBJECT_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;

class S3CopyResourceDefinitionGeneratorTest {
    
    private final S3CopyResourceDefinitionGenerator generator = new S3CopyResourceDefinitionGenerator();
    
    private final String region = "region";
    private final String bucket = "bucket";
    private final String object = "object";
    private final String keyName = "key";
    private final String endpoint = "http://endpoint";

    @Test
    void shouldReturnNull_whenNotSameType() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance().type("something else").build();
        var dataFlow = dataFlow(source, destination);
    
        var result = generator.generate(dataFlow);
    
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnNull_whenSameTypeAndNotSameEndpointOverride() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, "http://some-other-endpoint")
                .build();
        var dataFlow = dataFlow(source, destination);
    
        var result = generator.generate(dataFlow);
    
        assertThat(result).isNull();
    }
    
    @Test
    void shouldReturnNull_whenOnlySourceEndpointOverride() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .build();
        var dataFlow = dataFlow(source, destination);
        
        var result = generator.generate(dataFlow);
        
        assertThat(result).isNull();
    }
    
    @Test
    void shouldReturnNull_whenOnlyDestinationEndpointOverride() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var dataFlow = dataFlow(source, destination);
        
        var result = generator.generate(dataFlow);
        
        assertThat(result).isNull();
    }
    
    @Test
    void shouldGenerate() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).property(ENDPOINT_OVERRIDE, endpoint).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(OBJECT_NAME, object)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .keyName(keyName)
                .build();
        var dataFlow = dataFlow(source, destination);
    
        var definition = generator.generate(dataFlow);
        
        assertThat(definition).isNotNull();
        var newDestination = (DataAddress) definition.getProperty("newDestination");
        assertThat(newDestination.getKeyName()).isEqualTo(keyName);
        assertThat(newDestination.getStringProperty(REGION)).isEqualTo(region);
        assertThat(newDestination.getStringProperty(BUCKET_NAME)).isEqualTo(bucket);
        assertThat(newDestination.getStringProperty(OBJECT_NAME)).isEqualTo(object);
        assertThat(newDestination.getStringProperty(ENDPOINT_OVERRIDE)).isEqualTo(endpoint);
    }
    
    @Test
    void shouldGenerate_whenNoDestinationObjectName() {
        var sourceObject = "source-object";
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(OBJECT_NAME, sourceObject)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .keyName(keyName)
                .build();
        var dataFlow = dataFlow(source, destination);
        
        var definition = generator.generate(dataFlow);

        assertThat(definition).isNotNull();
        var newDestination = (DataAddress) definition.getProperty("newDestination");
        assertThat(newDestination.getKeyName()).isEqualTo(keyName);
        assertThat(newDestination.getStringProperty(REGION)).isEqualTo(region);
        assertThat(newDestination.getStringProperty(BUCKET_NAME)).isEqualTo(bucket);
        assertThat(newDestination.getStringProperty(OBJECT_NAME)).isEqualTo(sourceObject);
    }
    
    @Test
    void shouldGenerate_whenWithDestinationFolder() {
        var destinationFolder = "folder/";
        
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(FOLDER_NAME, destinationFolder)
                .property(OBJECT_NAME, object)
                .keyName(keyName)
                .build();
        var dataFlow = dataFlow(source, destination);
        
        var definition = generator.generate(dataFlow);
        
        assertThat(definition).isNotNull();
        var newDestination = (DataAddress) definition.getProperty("newDestination");
        assertThat(newDestination.getKeyName()).isEqualTo(keyName);
        assertThat(newDestination.getStringProperty(REGION)).isEqualTo(region);
        assertThat(newDestination.getStringProperty(BUCKET_NAME)).isEqualTo(bucket);
        assertThat(newDestination.getStringProperty(OBJECT_NAME)).isEqualTo(destinationFolder + object);
    }
    
    @Test
    void shouldGenerate_whenNoEndpointOverride() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(OBJECT_NAME, object)
                .keyName(keyName)
                .build();
        var dataFlow = dataFlow(source, destination);
    
        var definition = generator.generate(dataFlow);

        assertThat(definition).isNotNull();
        var newDestination = (DataAddress) definition.getProperty("newDestination");
        assertThat(newDestination.getKeyName()).isEqualTo(keyName);
        assertThat(newDestination.getStringProperty(REGION)).isEqualTo(region);
        assertThat(newDestination.getStringProperty(BUCKET_NAME)).isEqualTo(bucket);
        assertThat(newDestination.getStringProperty(OBJECT_NAME)).isEqualTo(object);
        assertThat(newDestination.getStringProperty(ENDPOINT_OVERRIDE)).isNull();
    }

    private DataFlow dataFlow(DataAddress source, DataAddress destination) {
        return DataFlow.Builder.newInstance()
                .source(source)
                .destination(destination)
                .build();
    }
}
