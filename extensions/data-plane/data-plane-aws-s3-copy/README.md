# S3 Copy Data Plane

## What the extension provides

This extension provides an addition to the S3 data plane, specifically for handling transfers from S3 to S3.
Typically, in the data plane source and sink are decoupled, which promotes flexibility, but does not provide the ideal
way of transferring data between 2 S3 buckets. 

This extension provides the possibility to copy a file between 2 S3 buckets (source and sink) without the data leaving
the AWS infrastructure, thereby potentially reducing high transfer costs.

## Relation to `data-plane-aws-s3`

This extension does not provide an alternative to the "standard" S3 data plane, but rather an addition. This extension
should be used together with the `data-plane-aws-s3` module, so that S3 is always supported, also when the
source/sink respectively is of another type.

## How to use

### Prerequisites

In order to be able to copy between 2 S3 buckets in different accounts, a specific setup including IAM roles and S3
bucket policies has to be in place. You can find AWS's guide on how exactly this can be achieved
[here](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/copy-data-from-an-s3-bucket-to-another-account-and-region-by-using-the-aws-cli.html).
Of course, this setup can be created manually before using the data plane, but this is a quite
tedious process and would need to be set-up separately for every S3-to-S3 transfer. To ease this process, a
[provisioning extension](../../control-plane/provision/provision-aws-s3-copy/README.md) has been created, which takes
care of setting up all required permissions for the copy operation as well as removing them after the transfer has
completed. It is recommended to use these modules together.

In order to use the provisioning extension, credentials for the AWS users have to be configured. How they need to be
configured for provider and consumer respectively is detailed in the
[provision extension's README](../../control-plane/provision/provision-aws-s3-copy/README.md#prerequisites)

### Transfer service selection

The S3 copy feature is implemented through a custom `TransferService`, meaning it does not rely on the
`PipelineService` and `DataSource`/`DataSink` structure. Thus, when both S3 data plane extensions are included in a 
runtime, there are 2 `TransferService` implementations capable of handling S3-to-S3 transfers. Therefore, the default
`TransferServiceSelectionStrategy` would not ensure the correct `TransferService` is chosen for S3-to-S3 transfers.
It is therefore recommended to use a different selection strategy, e.g. the one provided
[in this extension](../data-plane-transfer-service-selection/README.md).

### Data address

There is no difference in `DataAddress` and therefore `Asset` or `TransferRequest` properties in comparison to the
"standard" S3 data plane. Details on the `DataAddress` properties can be found in the
[data-plane-aws-s3 README](../data-plane-aws-s3/README.md#dataaddress-schema).

### Recommended usage

The following combination of modules is recommended to use the S3 copy feature:

Control plane:
```kotlin
implementation("org.eclipse.edc:provision:provision-aws-s3-copy:<version>")
```

Data plane:
```kotlin
implementation("org.eclipse.edc:provision:data-plane-aws-s3:<version>")
implementation("org.eclipse.edc:provision:data-plane-aws-s3-copy:<version>")
implementation("org.eclipse.edc:provision:data-plane-transfer-service-selection:<version>")
```

## Required AWS permissions

When used in conjunction with the `provision-aws-s3-copy` extension mentioned above, all necessary permissions/policies
for the copy operation will be set up during provisioning. The AWS users/roles used for the transfer therefore need
to satisfy only the permissions described in the
[provision extension's `README`](../../control-plane/provision/provision-aws-s3-copy/README.md#required-aws-permissions).
