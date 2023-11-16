## Aws S3 Data Plane module

### About this module

This module contains a Data Plane extension to copy data to and from Aws S3.

When as a source, it supports copying a single or multiple objects.

#### AmazonS3 DataAddress Configuration

The behavior of object transfers can be customized using `DataAddress` properties.

- When `keyPrefix` is present, transfer all objects with keys that start with the specified prefix.
- When `keyPrefix` is not present, transfer only the object with a key matching the `keyName` property.
- Precedence: `keyPrefix` takes precedence over `keyName` when determining which objects to transfer. It allows for both multiple object transfers and fetching a single object when necessary.

>Note: Using `keyPrefix` introduces an additional step to list all objects whose keys match the specified prefix.
 
#### Example Usage:

Configuration with `keyPrefix`:

```json
{
  "dataAddress": {
    "keyName": "my-key-name",
    "keyPrefix": "my-key-prefix/"
  }
}
```

#### AmazonS3 Chunk size Configuration
The maximum chunk of stream to be read, by default, is 500mb. It can be changed in the EDC config file as `edc.dataplane.aws.sink.chunk.size.mb` or in the env variables as `EDC_DATAPLANE_AWS_SINK_CHUNK_SIZE_MB`.