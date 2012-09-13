BSONSplitter
============

Splits large BSON dumps (Binary JSON) into smaller files.

**Usage**

`BSONSplit source.bson 100 C:\\output_folder`

**Arguments**

- Source (BSON) Filename
- Chunk size in MB
- Writable Folder to output splitted BSONs


**Dependencies** 

-  [MongoDB Java Driver](https://github.com/mongodb/mongo-java-driver/)