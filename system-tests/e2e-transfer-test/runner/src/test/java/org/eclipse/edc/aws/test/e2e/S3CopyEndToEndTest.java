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

package org.eclipse.edc.aws.test.e2e;

import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.junit.extensions.EmbeddedRuntime;
import org.eclipse.edc.junit.extensions.RuntimeExtension;
import org.eclipse.edc.junit.extensions.RuntimePerClassExtension;
import org.eclipse.edc.junit.testfixtures.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.AttachUserPolicyRequest;
import software.amazon.awssdk.services.iam.model.CreateAccessKeyRequest;
import software.amazon.awssdk.services.iam.model.CreatePolicyRequest;
import software.amazon.awssdk.services.iam.model.CreateUserRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetBucketPolicyRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.createAsset;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.createConsumerSecret;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.createContractDefinition;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.createPolicy;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.createProviderSecret;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.getAgreementId;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.initiateNegotiation;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.initiateTransfer;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.waitForNegotiationState;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.waitForProviderTransferState;
import static org.eclipse.edc.aws.test.e2e.EndToEndTestCommon.waitForTransferState;
import static org.eclipse.edc.connector.controlplane.contract.spi.types.negotiation.ContractNegotiationStates.FINALIZED;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.COMPLETED;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.DEPROVISIONED;

@Testcontainers
@EndToEndTest
class S3CopyEndToEndTest {
    
    private static final DockerImageName LOCALSTACK_DOCKER_IMAGE = DockerImageName.parse("localstack/localstack:3.5.0");
    
    private static final String TEST_RESOURCES = "system-tests/e2e-transfer-test/runner/src/test/resources/";
    
    private String fileContent = "Hello, world!";
    private String region = "eu-central-1";
    
    private String sourceBucket = "source-bucket";
    private String sourceObjectName = "source.txt";
    private String sourceUser = "source-user";
    private String sourceUserPolicyName = "source-user-policy";
    private String sourceAccessKeyId;
    private String sourceSecretAccessKey;
    
    private String destinationBucket = "destination-bucket";
    private String destinationObjectName = "transferred.txt";
    private String destinationUser = "destination-user";
    private String destinationUserPolicyName = "destination-user-policy";
    private String destinationAccessKeyId;
    private String destinationSecretAccessKey;
    
    @Container
    private static final LocalStackContainer LOCALSTACK_CONTAINER = new LocalStackContainer(LOCALSTACK_DOCKER_IMAGE)
            .withServices(
                    LocalStackContainer.Service.S3,
                    LocalStackContainer.Service.IAM,
                    LocalStackContainer.Service.STS);
    
    @RegisterExtension
    private static final RuntimeExtension PROVIDER = new RuntimePerClassExtension(new EmbeddedRuntime(
            "provider",
            Map.of("edc.fs.config", new File(TestUtils.findBuildRoot(), TEST_RESOURCES + "config/provider-config.properties").getAbsolutePath()),
            ":system-tests:e2e-transfer-test:runtime"
    ));
    
    @RegisterExtension
    private static final RuntimeExtension CONSUMER = new RuntimePerClassExtension(new EmbeddedRuntime(
            "consumer",
            Map.of("edc.fs.config", new File(TestUtils.findBuildRoot(), TEST_RESOURCES + "config/consumer-config.properties").getAbsolutePath()),
            ":system-tests:e2e-transfer-test:runtime"
    ));
    
    @BeforeEach
    void setUp() {
        LOCALSTACK_CONTAINER.start();
        
        var s3Client = getS3Client();
        var iamClient = getIamClient();
        
        // provider
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(sourceBucket)
                .build());
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(sourceBucket)
                .key(sourceObjectName)
                .build(), RequestBody.fromBytes(fileContent.getBytes()));
        iamClient.createPolicy(CreatePolicyRequest.builder()
                .policyName(sourceUserPolicyName)
                .policyDocument(sourceUserPolicy())
                .build());
        iamClient.createUser(CreateUserRequest.builder()
                .userName(sourceUser)
                .build());
        iamClient.attachUserPolicy(AttachUserPolicyRequest.builder()
                .userName(sourceUser)
                .policyArn("arn:aws:iam::000000000000:policy/" + sourceUserPolicyName)
                .build());
        var sourceCredentials = iamClient.createAccessKey(CreateAccessKeyRequest.builder()
                .userName(sourceUser)
                .build());
        sourceAccessKeyId = sourceCredentials.accessKey().accessKeyId();
        sourceSecretAccessKey = sourceCredentials.accessKey().secretAccessKey();
        
        // consumer
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(destinationBucket)
                .build());
        iamClient.createPolicy(CreatePolicyRequest.builder()
                .policyName(destinationUserPolicyName)
                .policyDocument(destinationUserPolicy())
                .build());
        iamClient.createUser(CreateUserRequest.builder()
                .userName(destinationUser)
                .build());
        iamClient.attachUserPolicy(AttachUserPolicyRequest.builder()
                .userName(destinationUser)
                .policyArn("arn:aws:iam::000000000000:policy/" + destinationUserPolicyName)
                .build());
        var destinationCredentials = iamClient.createAccessKey(CreateAccessKeyRequest.builder()
                .userName(destinationUser)
                .build());
        destinationAccessKeyId = destinationCredentials.accessKey().accessKeyId();
        destinationSecretAccessKey = destinationCredentials.accessKey().secretAccessKey();
    }
    
    @AfterEach
    void tearDown() {
        LOCALSTACK_CONTAINER.stop();
    }
    
    @Test
    void s3CopyTransfer() {
        createProviderSecret("source-user-access-key-id", sourceAccessKeyId);
        createProviderSecret("source-user-secret-access-key", sourceSecretAccessKey);
        
        createConsumerSecret("s3-credentials", awsSecretToken(destinationAccessKeyId, destinationSecretAccessKey));
        
        createAsset(LOCALSTACK_CONTAINER.getEndpoint().toString());
        createPolicy();
        createContractDefinition();
        
        var negotiationId = initiateNegotiation();
        waitForNegotiationState(negotiationId, FINALIZED);
        var agreementId = getAgreementId(negotiationId);
        
        var transferId = initiateTransfer(agreementId, LOCALSTACK_CONTAINER.getEndpoint().toString());
        waitForTransferState(transferId, COMPLETED);
        
        var destinationFileContent = readS3DestinationObject();
        assertThat(destinationFileContent).isEqualTo(fileContent);
        
        waitForProviderTransferState(transferId, DEPROVISIONED);
        
        var s3Client = getS3Client();
        assertThatThrownBy(() -> s3Client.getBucketPolicy(GetBucketPolicyRequest.builder()
                .bucket(destinationBucket)
                .build()))
                .isInstanceOfSatisfying(S3Exception.class, ex -> assertThat(ex.awsErrorDetails().errorCode().equals("NoSuchBucketPolicy")));
        
        var iamClient = getIamClient();
        assertThat(iamClient.listRoles().roles()).noneSatisfy(role -> assertThat(role.roleName().startsWith("edc")));
    }
    
    private String readS3DestinationObject() {
        var s3Client = getS3Client();
        var getObjectRequest = GetObjectRequest.builder()
                .bucket(destinationBucket)
                .key(destinationObjectName)
                .build();
        try {
            var objectStream = s3Client.getObject(getObjectRequest);
            return new String(objectStream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    private S3Client getS3Client() {
        return S3Client.builder()
                .credentialsProvider(localStackCredentials())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpoint())
                .region(Region.of(region))
                .build();
    }
    
    private IamClient getIamClient() {
        return IamClient.builder()
                .credentialsProvider(localStackCredentials())
                .endpointOverride(LOCALSTACK_CONTAINER.getEndpoint())
                .build();
    }
    
    private StaticCredentialsProvider localStackCredentials() {
        return StaticCredentialsProvider.create(AwsBasicCredentials
                .create(LOCALSTACK_CONTAINER.getAccessKey(), LOCALSTACK_CONTAINER.getSecretKey()));
    }
    
    private String sourceUserPolicy() {
        return "{\n"
                + "    \"Version\": \"2012-10-17\",\n"
                + "    \"Statement\": [\n"
                + "        {\n"
                + "            \"Sid\": \"iamPermissions\",\n"
                + "            \"Effect\": \"Allow\",\n"
                + "            \"Action\": [\n"
                + "                \"iam:DeleteRolePolicy\",\n"
                + "                \"iam:TagRole\",\n"
                + "                \"iam:CreateRole\",\n"
                + "                \"iam:DeleteRole\",\n"
                + "                \"iam:PutRolePolicy\",\n"
                + "                \"iam:GetUser\"\n"
                + "            ],\n"
                + "            \"Resource\": [\n"
                + "                \"arn:aws:iam::000000000000:role/*\",\n"
                + "                \"arn:aws:iam::000000000000:user/" + sourceUser + "\"\n"
                + "            ]\n"
                + "        },\n"
                + "        {\n"
                + "            \"Sid\": \"stsPermissions\",\n"
                + "            \"Effect\": \"Allow\",\n"
                + "            \"Action\": \"sts:AssumeRole\",\n"
                + "            \"Resource\": \"arn:aws:iam::000000000000:role/*\"\n"
                + "        }\n"
                + "    ]\n"
                + "}";
    }
    
    private String destinationUserPolicy() {
        return "{\n"
                + "    \"Version\": \"2012-10-17\",\n"
                + "    \"Statement\": [\n"
                + "        {\n"
                + "            \"Sid\": \"s3Permissions\",\n"
                + "            \"Effect\": \"Allow\",\n"
                + "            \"Action\": [\n"
                + "                \"s3:PutBucketPolicy\",\n"
                + "                \"s3:GetBucketPolicy\",\n"
                + "                \"s3:DeleteBucketPolicy\"\n"
                + "            ],\n"
                + "            \"Resource\": \"arn:aws:s3:::" + destinationBucket + "\"\n"
                + "        }\n"
                + "    ]\n"
                + "}";
    }
    
    private String awsSecretToken(String accessKeyId, String secretAccessKey) {
        return "{\\\"edctype\\\":\\\"dataspaceconnector:secrettoken\\\",\\\"accessKeyId\\\":\\\"" + accessKeyId + "\\\",\\\"secretAccessKey\\\":\\\"" + secretAccessKey + "\\\"}";
    }
}
