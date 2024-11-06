package org.eclipse.edc.connector.provision.aws.s3.copy;

import org.eclipse.edc.aws.s3.spi.S3BucketSchema;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProviderResourceDefinitionGenerator;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.jetbrains.annotations.Nullable;

import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.BUCKET_NAME;
import static org.eclipse.edc.aws.s3.spi.S3BucketSchema.REGION;

public class CrossAccountCopyResourceDefinitionGenerator implements ProviderResourceDefinitionGenerator {
    @Override
    public @Nullable ResourceDefinition generate(TransferProcess transferProcess, DataAddress assetAddress, Policy policy) {
        var bucketPolicyStatementSid = "edc-transfer_" + transferProcess.getId(); //TODO
        
        return CrossAccountCopyResourceDefinition.Builder.newInstance()
                .destinationRegion(transferProcess.getDataDestination().getStringProperty(REGION))
                .destinationBucketName(transferProcess.getDataDestination().getStringProperty(BUCKET_NAME))
                .destinationKeyName(transferProcess.getDataDestination().getKeyName())
                .bucketPolicyStatementSid(bucketPolicyStatementSid)
                .sourceDataAddress(transferProcess.getContentDataAddress())
                .build();
    }
    
    @Override
    public boolean canGenerate(TransferProcess transferProcess, DataAddress assetAddress, Policy policy) {
        // only applicable for S3-to-S3 transfer
        return S3BucketSchema.TYPE.equals(transferProcess.getContentDataAddress().getType())
                && S3BucketSchema.TYPE.equals(transferProcess.getDestinationType());
    }
}
