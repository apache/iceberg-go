
## Feature Support / Roadmap

### FileSystem Support

| Filesystem Type      | Supported |
| :------------------: | :-------: |
| S3                   |    X      |
| Google Cloud Storage |           |
| Azure Blob Storage   |           |
| Local Filesystem     |    X      |

### Metadata

| Operation                | Supported |
| :----------------------- | :-------: |
| Get Schema               |     X     |
| Get Snapshots            |     X     |
| Get Sort Orders          |     X     |
| Get Partition Specs      |     X     |
| Get Manifests            |     X     |
| Create New Manifests     |     X     |
| Plan Scan                |     x     |
| Plan Scan for Snapshot   |     x     |

### Catalog Support

| Operation                | REST | Hive | DynamoDB | Glue |
| :----------------------- | :--: | :--: | :------: | :--: |
| Load Table               |  X   |      |          |  X   |
| List Tables              |  X   |      |          |  X   |
| Create Table             |  X   |      |          |  X   |
| Update Current Snapshot  |      |      |          |      |
| Create New Snapshot      |      |      |          |      |
| Rename Table             |  X   |      |          |  X   |
| Drop Table               |  X   |      |          |  X   |
| Alter Table              |      |      |          |      |
| Set Table Properties     |      |      |          |      |
| Create Namespace         |  X   |      |          |  X   |
| Drop Namespace           |  X   |      |          |  X   |
| Set Namespace Properties |  X   |      |          |  X   |

### Read/Write Data Support

* No intrinsic support for writing data yet.
* Plan to add [Apache Arrow](https://pkg.go.dev/github.com/apache/arrow-go/) support eventually.
* Data can currently be read as an Arrow Table or as a stream of Arrow record batches.

# Get in Touch

- [Iceberg community](https://iceberg.apache.org/community/)