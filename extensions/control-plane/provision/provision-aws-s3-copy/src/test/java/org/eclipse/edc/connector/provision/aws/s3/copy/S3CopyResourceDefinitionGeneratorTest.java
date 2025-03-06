package org.eclipse.edc.connector.provision.aws.s3.copy;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
    private final Policy policy = Policy.Builder.newInstance().build();
    
    @Test
    void canGenerate_sameTypeAndNoEndpointOverride_shouldReturnTrue() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var transferProcess = transferProcess(source, destination);
        
        var result = generator.canGenerate(transferProcess, source, policy);
        
        assertThat(result).isTrue();
    }
    
    @Test
    void canGenerate_notSameType_shouldReturnFalse() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance().type("something else").build();
        var transferProcess = transferProcess(source, destination);
    
        var result = generator.canGenerate(transferProcess, source, policy);
    
        assertThat(result).isFalse();
    }
    
    @Test
    void canGenerate_sameTypeAndSameEndpointOverride_shouldReturnTrue() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var transferProcess = transferProcess(source, destination);
    
        var result = generator.canGenerate(transferProcess, source, policy);
    
        assertThat(result).isTrue();
    }
    
    @Test
    void canGenerate_sameTypeAndNotSameEndpointOverride_shouldReturnFalse() {
        var source = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(ENDPOINT_OVERRIDE, "http://some-other-endpoint")
                .build();
        var transferProcess = transferProcess(source, destination);
    
        var result = generator.canGenerate(transferProcess, source, policy);
    
        assertThat(result).isFalse();
    }
    
    @Test
    void generate_shouldGenerateResourceDefinition() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(OBJECT_NAME, object)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .keyName(keyName)
                .build();
        var transferProcess = transferProcess(source, destination);
    
        var definition = generator.generate(transferProcess, source, policy);
        
        assertThat(definition)
                .isNotNull()
                .isInstanceOf(S3CopyResourceDefinition.class);
        var s3CopyDefinition = (S3CopyResourceDefinition) definition;
        assertThat(s3CopyDefinition.getDestinationRegion()).isEqualTo(region);
        assertThat(s3CopyDefinition.getDestinationBucketName()).isEqualTo(bucket);
        assertThat(s3CopyDefinition.getDestinationObjectName()).isEqualTo(object);
        assertThat(s3CopyDefinition.getDestinationKeyName()).isEqualTo(keyName);
        assertThat(s3CopyDefinition.getEndpointOverride()).isEqualTo(endpoint);
        assertThat(s3CopyDefinition.getBucketPolicyStatementSid()).isNotNull().startsWith("edc-transfer_");
        assertThat(s3CopyDefinition.getId()).isNotNull().satisfies(UUID::fromString);
    }
    
    @Test
    void generate_withDestinationFolder_shouldGenerateResourceDefinition() {
        var destinationFolder = "folder/";
        
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(FOLDER_NAME, destinationFolder)
                .property(OBJECT_NAME, object)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .keyName(keyName)
                .build();
        var transferProcess = transferProcess(source, destination);
        
        var definition = generator.generate(transferProcess, source, policy);
        
        assertThat(definition)
                .isNotNull()
                .isInstanceOf(S3CopyResourceDefinition.class);
        var s3CopyDefinition = (S3CopyResourceDefinition) definition;
        assertThat(s3CopyDefinition.getDestinationRegion()).isEqualTo(region);
        assertThat(s3CopyDefinition.getDestinationBucketName()).isEqualTo(bucket);
        assertThat(s3CopyDefinition.getDestinationObjectName()).isEqualTo(destinationFolder + object);
        assertThat(s3CopyDefinition.getDestinationKeyName()).isEqualTo(keyName);
        assertThat(s3CopyDefinition.getEndpointOverride()).isEqualTo(endpoint);
        assertThat(s3CopyDefinition.getBucketPolicyStatementSid()).isNotNull().startsWith("edc-transfer_");
        assertThat(s3CopyDefinition.getId()).isNotNull().satisfies(UUID::fromString);
    }
    
    @Test
    void generate_noEndpointOverride_shouldGenerateResourceDefinition() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(OBJECT_NAME, object)
                .keyName(keyName)
                .build();
        var transferProcess = transferProcess(source, destination);
    
        var definition = generator.generate(transferProcess, source, policy);
    
        assertThat(definition)
                .isNotNull()
                .isInstanceOf(S3CopyResourceDefinition.class);
        var s3CopyDefinition = (S3CopyResourceDefinition) definition;
        assertThat(s3CopyDefinition.getDestinationRegion()).isEqualTo(region);
        assertThat(s3CopyDefinition.getDestinationBucketName()).isEqualTo(bucket);
        assertThat(s3CopyDefinition.getDestinationObjectName()).isEqualTo(object);
        assertThat(s3CopyDefinition.getDestinationKeyName()).isEqualTo(keyName);
        assertThat(s3CopyDefinition.getEndpointOverride()).isNull();
        assertThat(s3CopyDefinition.getBucketPolicyStatementSid()).isNotNull().startsWith("edc-transfer_");
        assertThat(s3CopyDefinition.getId()).isNotNull().satisfies(UUID::fromString);
    }
    
    @Test
    void generate_noDestinationRegion_shouldThrowException() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(BUCKET_NAME, bucket)
                .property(OBJECT_NAME, object)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .keyName(keyName)
                .build();
        var transferProcess = transferProcess(source, destination);
    
        assertThatThrownBy(() -> generator.generate(transferProcess, source, policy))
                .isInstanceOf(NullPointerException.class);
    }
    
    @Test
    void generate_noDestinationBucketName_shouldThrowException() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(OBJECT_NAME, object)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .keyName(keyName)
                .build();
        var transferProcess = transferProcess(source, destination);
    
        assertThatThrownBy(() -> generator.generate(transferProcess, source, policy))
                .isInstanceOf(NullPointerException.class);
    }
    
    @Test
    void generate_noDestinationObjectName_shouldThrowException() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .keyName(keyName)
                .build();
        var transferProcess = transferProcess(source, destination);
        
        assertThatThrownBy(() -> generator.generate(transferProcess, source, policy))
                .isInstanceOf(NullPointerException.class);
    }
    
    @Test
    void generate_noDestinationKeyName_shouldThrowException() {
        var source = DataAddress.Builder.newInstance().type(S3BucketSchema.TYPE).build();
        var destination = DataAddress.Builder.newInstance()
                .type(S3BucketSchema.TYPE)
                .property(REGION, region)
                .property(BUCKET_NAME, bucket)
                .property(OBJECT_NAME, object)
                .property(ENDPOINT_OVERRIDE, endpoint)
                .build();
        var transferProcess = transferProcess(source, destination);
    
        assertThatThrownBy(() -> generator.generate(transferProcess, source, policy))
                .isInstanceOf(NullPointerException.class);
    }
    
    private TransferProcess transferProcess(DataAddress source, DataAddress destination) {
        return TransferProcess.Builder.newInstance()
                .contentDataAddress(source)
                .dataDestination(destination)
                .build();
    }
}
