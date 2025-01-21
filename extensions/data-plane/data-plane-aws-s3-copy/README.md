# S3 Copy Data Plane

## What the extension provides

This extension provides an addition to the S3 data plane, specifically for handling transfers from S3 to S3.
Typically, in the data plane source and sink are decoupled, which promotes flexibility, but does not provide the ideal
way of transferring data between 2 S3 buckets. 

This extension provides the possibility to copy a file between 2 S3 buckets (source and sink) without the data leaving
the AWS infrastructure, thereby potentially reducing high transfer costs.

## How to use

### Prerequisites

In order to be able to copy between 2 S3 buckets in different accounts, a specific setup including IAM roles and S3
bucket policies has to be in place. You can find AWS's guide on how exactly this can be achieved
[here](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/copy-data-from-an-s3-bucket-to-another-account-and-region-by-using-the-aws-cli.html).

Of course, this setup can be created manually before using the data plane to copy the file(s), but this is a quite
tedious process and would need to be set-up separately for every S3-to-S3 transfer. To ease this process, a
[provisioning extension](../../control-plane/provision/provision-aws-s3-copy/README.md) has been created, which takes
care of setting up all required permissions for the copy operation as well as removing them after the transfer has
completed. It is recommended to use these modules together.

### Relation to `data-plane-aws-s3`

This extension does not provide an alternative to the "standard" S3 data plane, but rather an addition. This extension
should always be used together with the `data-plane-aws-s3` module, so that S3 is always supported as a source and
sink type, also when the source/sink respectively is of another type.

If you want to use this data plane extension on its own for some reason, you will need to manually register the
data plane with the control plane, as the self-registration feature does not work for this extension.

### Transfer service selection

The S3 copy feature is implemented through a custom `TransferService`, meaning it does not rely on the
`PipelineService` and `DataSource`/`DataSink` structure. Thus, when both S3 data plane extensions are included in a 
runtime, there are 2 `TransferService` implementations capable of handling S3-to-S3 transfers. Therefore, the default
`TransferServiceSelectionStrategy` would not ensure the correct `TransferService` is chosen for S3-to-S3 transfers.
It is therefore recommended to use a different selection strategy, e.g. the one provided
[in this extension](../data-plane-transfer-service-selection/README.md).

### Data address

There is no difference in `DataAddress` and therefore `TransferRequest` properties in comparison to the "standard"
S3 data plane.

### Recommended usage

The following combination of modules is recommended to use the S3 copy feature:

```kotlin
implementation("org.eclipse.edc:provision:provision-aws-s3-copy:<version>")
implementation("org.eclipse.edc:provision:data-plane-aws-s3:<version>")
implementation("org.eclipse.edc:provision:data-plane-aws-s3-copy:<version>")
implementation("org.eclipse.edc:provision:data-plane-transfer-service-selection:<version>")
```

In addition, the `accessKeyId` and `secretAccessKey` of the AWS user need to be present in the vault. 
The keys under which they are stored in the vault need to be set in the configuration using the following properties:

```properties
edc.aws.access.key=<vault-key-of-access-key-id>
edc.aws.secret.access.key=<vault-key-of-secret-access-key>
```

## Required AWS permissions

When used in conjunction with the `provision-aws-s3-copy` extension mentioned above, all necessary permissions/policies
for the copy operation will be set up during provisioning. The AWS users/roles used for the transfer therefore need
to satisfy only the permissions described in the
[provision extension's `README`](../../control-plane/provision/provision-aws-s3-copy/README.md#required-aws-permissions).

If you want to use this data plane extension without the provisioning and want to set up everything manually, you
will need a cross-account role in the provider account (with read access on the source bucket and write access on the
destination bucket) and a bucket policy on the destination bucket, which allows the role in the provider account to
write to the bucket. Details on how the policies need to look can be found in the AWS documentation.
