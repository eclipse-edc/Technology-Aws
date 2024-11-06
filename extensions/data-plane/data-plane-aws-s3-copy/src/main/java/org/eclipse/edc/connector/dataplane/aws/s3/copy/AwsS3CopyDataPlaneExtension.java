package org.eclipse.edc.connector.dataplane.aws.s3.copy;

import org.eclipse.edc.aws.s3.AwsClientProvider;
import org.eclipse.edc.connector.dataplane.spi.registry.TransferServiceRegistry;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;

public class AwsS3CopyDataPlaneExtension implements ServiceExtension {
    
    @Inject
    private AwsClientProvider clientProvider;
    @Inject
    private TransferServiceRegistry registry;
    @Inject
    private Monitor monitor;
    @Inject
    private Vault vault;
    @Inject
    private TypeManager typeManager;
    @Inject
    private DataAddressValidatorRegistry validator;
    
    @Override
    public void initialize(ServiceExtensionContext context) {
        var s3CopyTransferService = new AwsS3CopyTransferService(clientProvider, monitor, vault, typeManager, validator);
        registry.registerTransferService(s3CopyTransferService);
    }
}
