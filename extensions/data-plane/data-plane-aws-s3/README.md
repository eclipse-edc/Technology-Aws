# Aws S3 Data Plane module

## About this module

This module contains a Data Plane extension to copy data to and from Aws S3.

When as a source, it supports copying a single or multiple objects.

## DataAddress Schema

### Properties

| Key                | Description                                                            | Applies at              | Mandatory                                             |
|:-------------------|:-----------------------------------------------------------------------|-------------------------|-------------------------------------------------------|
| `type`             | Defines the Asset type ( `AmazonS3` )                                  | `source`, `destination` | `true`                                                |
| `region`           | Defines the region of the bucket (`us-east-1`, `eu-west-1` ...)        | `source`, `destination` | `true`                                                |
| `endpointOverride` | Defines a custom endpoint URL                                          | `source`, `destination` | `false`                                               |
| `bucketName`       | Defines the name of the S3 bucket                                      | `source`, `destination` | `true`                                                |
| `objectName`       | Defines the name of the S3 object                                      | `source`, `destination` | `true` (in `source` if `objectPrefix` is not present) |
| `objectPrefix`     | Defines the prefix of the S3 objects to be fetched ( `objectPrefix/` ) | `source`                | `true` (if `objectName` is not present)               |
| `folderName`       | Defines the folder name for S3 objects to be grouped ( `folderName/` ) | `source`, `destination` | `false`                                               |
| `keyName`          | Defines the `vault` entry containing the secret token/credentials      | `source`, `destination` | `false`                                               |
| `accessKeyId`      | Defines the access key id to access S3 Bucket/Object                   | `source`, `destination` | `false`                                               |
| `secretAccessKey`  | Defines the secret access key id to access S3 Bucket/Object            | `source`, `destination` | `false`                                               |

### S3DataSource Properties and behavior

The behavior of object transfers can be customized using `DataAddress` properties.

- There are three different ways to select objects by specifying a `folderName` and/or a `objectPrefix`.
  Objects in an S3 bucket are persisted under the same root directory (the bucket), and a structured organization is 
  achievable by using object key prefixes. There are cases where maintaining the key prefix in the data destination 
  is desirable but other cases where it's not.
  When using `folderName`, one can aggregate objects contained within a folder like structure. The `folderName` part will
  be removed in the destination.
  Using `objectPrefix`, one can aggregate objects whose key is prefixed by the specified string. The property can still
  be used for folder like aggregation but the prefix part will not be removed in the destination.
  When used in combination, both properties will be used for object selection through the concatenation of `folderName` 
  with `objectPrefix` (`folderName` + `objectPrefix`). Similarly, the `folderName` part will be removed from the object name 
  in the destination.
- When `folderName` attribute is requested as slash (folderName = "/") and `objectPrefix` is empty or null an entire s3 
  bucket transfer is triggered.
- When `folderName` and `objectPrefix` are not present, transfer only the object with a key matching the `objectName`
  property.
- Precedence: `folderName` and/or `objectPrefix` take precedence over `objectName` when determining which objects to
  transfer. It allows for both multiple object transfers and fetching a single object when necessary.

> Note: Using `folderName` or/and `objectPrefix` introduces an additional step to list all objects whose keys match the 
  specified "filter".

### S3DataSink Properties and behavior

The destination's object naming can be tailored further through the utilization of `DataAddress` properties.

- The `objectName` property allows specifying the desired name for the object in the destination. It comes into effect
  when the `DataSource` comprises a single Part or object. If there are multiple `Part`s or the property is undefined,
  the name of the `Part` (source object name) will be used.
- The `folderName` property can consistently group objects in the destination, whether there is a single object or
  multiple objects.

### Secret Resolution

The `keyName` property should point to a `vault` entry that contains a JSON-serialized `SecretToken` object. The
possible values are:

- `AwsSecretToken`: Using `accessKeyId` and `secretAccessKey`, it's a form of basic AWS access key authentication. This
  method relies on a set of long-term credentials (access key ID and secret access key) associated with an IAM user or
  role.
  ```json
  {
    "edctype": "dataspaceconnector:secrettoken",
    "accessKeyId": "<ACCESS_KEY_ID>",
    "secretAccessKey": "<SECRET_ACCESS_KEY>"
  }
  ```
- `AwsTemporatySecretToken`: Has a `sessionToken` in addition to the `accessKeyId` and `secretAccessKey`, it is
  typically
  referred to as AWS temporary security credentials. This process involves assuming an IAM role to obtain short-term
  credentials, which include an `accessKeyId`, `secretAccessKey`, and a session token. In addition to these fields,
  the token expiration time, which is received together with the credentials upon assuming a role or otherwise
  requesting temporary credentials, has to be specified as a **unix timestamp**.
  ```json
  {
    "edctype": "dataspaceconnector:secrettoken",
    "accessKeyId": "<ACCESS_KEY_ID>",
    "secretAccessKey": "<SECRET_ACCESS_KEY>",  
    "sessionToken": "<SESSION_TOKEN>",
    "expiration": "<EXPIRATION>"
  }
  ```

Example:
```json
{
  "dataAddress": {
    "type": "AmazonS3",
    "bucketName": "bucketName",
    "region": "us-east-1",
    "objectName": "test/object.bin",
    "keyName": "<SECRET_KEY_IN_VAULT>"
  }
}
```

### Plain text credentials

This feature has been introduced to provide flexibility by not mandating the use of a `vault`. However, it is important
to note that this functionality is not recommended for production environments.

The properties `accessKeyId` and `secretAccessKey`, can be used for basic AWS access key authentication.

Example:
```json
{
  "dataAddress": {
    "type": "AmazonS3",
    "bucketName": "bucketName",
    "region": "us-east-1",
    "objectName": "test/object.bin",
    "accessKeyId": "<ACCESS_KEY_ID>",
    "secretAccessKey": "<SECRET_ACCESS_KEY>"
  }
}
```

### Data Address Examples

#### Source - Data Address Example

- Single object:
```json
{
  "dataAddress": {
    "type": "AmazonS3",
    "bucketName": "bucketName",
    "region": "us-east-1",
    "objectName": "test/object.bin",
    "keyName": "(see above)"
  }
}
```
- Multiple objects:
```json
{
  "dataAddress": {
    "type": "AmazonS3",
    "bucketName": "bucketName",
    "region": "us-east-1",
    "objectPrefix": "test/",
    "keyName": "(see above)"
  }
}
```

#### Destination - Data Address Example

- Single object:
```json
{
  "dataDestination": {
    "type": "AmazonS3",
    "bucketName": "bucketName",
    "region": "us-east-1",
    "folderName": "destinationFolder/",
    "objectName": "newName",
    "keyName": "(see above)"
  }
}
```

- Multiple objects:
```json
{
  "dataDestination": {
    "type": "AmazonS3",
    "bucketName": "bucketName",
    "region": "us-east-1",
    "folderName": "destinationFolder/",
    "keyName": "(see above)"
  }
}
```

## Required AWS permissions

The secrets described above should contain credentials for a user/role with the following permissions.

### Source

In order to be able to use an S3 bucket as the source of a transfer, the user/role needs the following permissions:
- `s3:ListBucket` on the bucket (only applies if `objectPrefix` is used)
- `s3:GetObject` on the object(s) in the bucket

Example:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "my-statement",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::<bucket-name>",
        "arn:aws:s3:::<bucket-name>/*"
      ]
    }
  ]
}
```

### Destination

In order to be able to use an S3 bucket as the destination of a transfer, the user/role needs the following permissions:
- `s3:putObject` on the object(s) in the bucket

Example:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "my-statement",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": [
        "arn:aws:s3:::<bucket-name>/*"
      ]
    }
  ]
}
```

## Configuration

### AmazonS3 Chunk size Configuration

The maximum chunk of stream to be read, by default, is 500mb. It can be changed in the EDC config file
as `edc.dataplane.aws.sink.chunk.size.mb` or in the env variables as `EDC_DATAPLANE_AWS_SINK_CHUNK_SIZE_MB`.
