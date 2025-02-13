# S3-to-S3 transfers within AWS

## Decision

We will provide an addition to the S3 data plane to enable the execution of S3-to-S3 transfers within the AWS
infrastructure, i.e. without going through the data plane.

## Rationale

With the way the existing S3 data plane is implemented, it reads the data from the source bucket first and then writes
it to the destination bucket. This is great for decoupling between source and destination, and allowing for transfers
between S3 and other storage types. But in the case that both source and destination are S3 buckets, it causes higher
than necessary transfer costs, as typically, moving data out of the AWS infrastructure is rather expensive. By copying
data between the buckets within AWS, transfer costs can be reduced.

## Approach

### Cross-account transfers in AWS

In AWS, any direct S3 transfer between different accounts (disregarding of whether a simple S3-copy is executed or a
service like `DataSync` is used) requires a specific permissions setup between the two accounts. The
[recommended approach](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/copy-data-from-an-s3-bucket-to-another-account-and-region-by-using-the-aws-cli.html)
for facilitating transfers between S3 buckets in different AWS accounts A & B is the following:
- in account A: create a role with a role policy that allows read access on the S3 bucket in one account and write access on the S3 bucket in another account
- in account B: update the bucket policy of the S3 bucket to allow the role in account A to read/write
- in account A: assume the role and initiate the transfer

Data is then copied from one bucket to the other across AWS accounts, without ever leaving the AWS infrastructure.

*In the recommended approach linked above, the role is created in the destination account, allowing the consumer to read
from the provider bucket. For the EDC implementation, these roles will be reversed.*

### Implementation

Implementing this approach in the EDC consists of mainly 2 parts:
- a dedicated `TransferService`, which will initiate an S3 copy
- provisioning-classes, which set up the required role & permission structure

Since the `TransferService` will simply need to create an S3 client and call the copy operation, the main part of
the implementation will be the provisioning. The implementation of the provisioner will be aligned with the
implementation of the existing S3 provisioning extension (using a `ProvisionPipeline` and `DeprovisionPipeline`).

#### Provision & transfer sequence

The sequence for the transfer (including provisioning) will be as follows:
1. Consumer sends transfer request to provider (type: `AmazonS3-PUSH`)
2. If the source address's type is also `AmazonS3`, the provider provisioning goes through the following steps:  
   2.1 Get the current AWS user  
   2.2 Create a role with a trust policy that allows the current user to assume the role (role tags:  
    - created-by: EDC  
    - edc:component-id: <component-id>  
    - edc:transfer-process-id: <transfer-process-id>)  
   2.3 Add a role policy to the role that allows reading the source object and writing the destination object  
   2.4 Get the destination bucket policy  
   2.5 Update the destination bucket policy with a statement to allow the previously created role to write to the bucket  
   2.6 Assume the role and return the credentials as part of the `ProvisionedResource`
3. Transfer is delegated to the data plane  
   3.1 Select the dedicated `TransferService`  
   3.2 Create an S3 client using the credentials of the assumed role  
   3.3 Invoke the copy operation to copy the source object to the destination bucket
4. After completion, deprovisioning is initiated:  
   4.1 Get the destination bucket policy  
   4.2 Filter for the statement added during provisioning by `Sid` and remove it  
   4.3 Update the destination bucket policy (if there are no other statements, the bucket policy is deleted, as when using the SDK, it is not possible to update a bucket policy without statements (even though this works fine in the AWS console))  
   4.4 Delete the role policy of the created role  
   4.5 Delete the role

After deprovisioning finishes, the AWS accounts are left in a clean state.

*Note: all provisioning will be done by the provider. This also includes updating the destination bucket policy.
While this is not ideal, there is currently no way for the consumer to take care of this, as both provider and consumer
need information from the other party (source account role needs to reference destination bucket, destination bucket
policy needs to reference source account role).*

### Testing

In addition to unit tests for all relevant classes, this feature will be tested end-to-end using LocalStack, as
LocalStack supports all required AWS APIs (S3, IAM, STS). This way, the integration between provisioning and transfer
service can be tested and correct clean-up through deprovisioning can be verified.
