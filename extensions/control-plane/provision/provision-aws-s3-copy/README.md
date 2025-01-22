# S3 Copy Provision

## What the extension provides

This extension provides support for provisioning the AWS setup required to perform a cross-account transfer of
S3 objects within the AWS infrastructure. It is therefore developed for use with the 
[respective data plane extension](../../../data-plane/data-plane-aws-s3-copy/README.md). **This provision extension
does not provide any value for transfers between different source/destination types as well as S3-to-S3 transfers
handled by the "standard" S3 data plane!** It sets up precisely the permissions needed to allow for copying S3
objects between two accounts.

### Provisioning sequence

In order to be able to copy between 2 S3 buckets in different accounts, a specific setup including IAM roles and S3
bucket policies has to be in place. You can find AWS's guide on how exactly this can be achieved
[here](https://docs.aws.amazon.com/prescriptive-guidance/latest/patterns/copy-data-from-an-s3-bucket-to-another-account-and-region-by-using-the-aws-cli.html).

**Note: As both accounts require information from the other account, all provisioning will be handled by the provider,
including updating the bucket policy on the consumer's destination bucket!**

During the provisioning phase, this extension will perform the following provision steps for S3-to-S3 transfers
(i.e. both source and destination data address are of type `AmazonS3`):
1. Get the current IAM user
2. Create and tag an IAM role, which the current user may assume
3. Add a role policy, allowing read operations on the source bucket and write operations on the destination bucket
4. Get the bucket policy of the destination bucket
5. Add a statement to the destination bucket policy, allowing the previously created role to write objects into the bucket
6. Assume the role

> The following tags will be added to the role:
>- `created-by`: `EDC`
>- `edc:component-id`: `<component-id>`
>- `edc:transfer-process-id`: `<transfer-process-id>`

The data plane can then use the credentials of the assumed role to perform a copy operation between source and
destination bucket. After a transfer has completed (or terminated), the following deprovision steps will be
performed:
1. Get the destination bucket policy
2. Remove the previously added statement from the bucket policy (or delete the bucket policy if no statements are left)
3. Delete the role policy
4. Delete the role

After deprovisioning, both AWS accounts will have the same state on the related resources that they had prior to the
provisioning process.

## How to use

As mentioned above, this extension should be used together with the `data-plane-aws-s3-copy` extension.

### Prerequisites

The only prerequisites for using this extension are existing source and destination buckets as well as AWS users/roles
in the source and destination accounts with the respective [necessary permissions](#required-aws-permissions).

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

In the following, the required AWS permissions for the users used on provider and consumer side are listed.

### Provider
- iam:GetUser (on own user)
- iam:CreateRole (on roles where the name starts with `edc-transfer_`)
- iam:TagRole (on roles where the name starts with `edc-transfer_`)
- iam:AssumeRole (on roles where the name starts with `edc-transfer_` or roles with tag)
- iam:PutRolePolicy (on roles where the name starts with `edc-transfer_` or roles with tag)
- iam:DeleteRolePolicy (on roles where the name starts with `edc-transfer_` or roles with tag)
- iam:DeleteRole (on roles where the name starts with `edc-transfer_` or roles with tag)

#### Example IAM policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "iam:GetUser"
            ],
            "Resource": [
                "arn:aws:iam::<account>:user/<username>"
            ]
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "iam:CreateRole",
                "iam:TagRole"
            ],
            "Resource": [
                "arn:aws:iam::<account>:role/edc-transfer_*"
            ]
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": [
                "sts:AssumeRole",
                "iam:PutRolePolicy",
                "iam:DeleteRolePolicy",
                "iam:DeleteRole"
            ],
            "Resource": "arn:aws:iam::<account>:role/*",
            "Condition": {
                "StringEquals": {
                    "aws:ResourceTag/created-by": "EDC"
                }
            }
        }
    ]
}
```

### Consumer
- s3:GetBucketPolicy (on destination bucket)
- s3:PutBucketPolicy (on destination bucket)
- s3:DeleteBucketPolicy (on destination bucket)

#### Example

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketPolicy",
                "s3:PutBucketPolicy",
                "s3:DeleteBucketPolicy"
            ],
            "Resource": "arn:aws:s3:::<destination-bucket-name>"
        }
    ]
}
```
