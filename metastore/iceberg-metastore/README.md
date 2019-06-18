# Iceberg Metastore

Iceberg Metastore implementation is based on [Iceberg table](http://iceberg.incubator.apache.org). 
Iceberg table schema (field names and types) is based on `MetadataUnit` class fields 
annotated with `MetastoreFieldDefinition` annotation. Data is partitioned by 
storage plugin, workspace, table name and metadata key.

## Configuration

Iceberg table configuration is indicated in `drill-metastore-module.conf` and 
can be overwritten in `drill-metastore-distrib.conf` or  `drill-metastore-override.conf` files.

`drill.metastore.config.properties` - allows to specify properties connected with file system.

`drill.metastore.iceberg.location.base_path` and `drill.metastore.iceberg.location.relative_path` -
indicates Iceberg table location base and relative paths.

`drill.metastore.table.properties` - allows to specify Iceberg table properties, refer to 
`org.apache.iceberg.TableProperties` class for the list of available table properties.

## Iceberg Table Location

Iceberg table resides on the file system in the location indicated in
`drill.metastore.iceberg.location` property. 

Note: Iceberg table supports concurrent writes and transactions 
but they are only effective on file systems that support atomic rename.

Inside given location, Iceberg table will create `metadata` folder where 
it will store its snapshots, manifests and table metadata.

## Metastore Data Location

Metastore metadata will be stored inside Iceberg table location provided
in the configuration file. Drill table metadata location will be constructed based on
storage plugin, workspace and table name: unique table identifier in Drill.

Assume Iceberg table location is `/drill/metastore/iceberg`, metadata for the table
`dfs.tmp.nation` will be stored in the `/drill/metastore/iceberg/dfs/tmp/nation` folder.

### Metadata Storage Format

By default, Metadata will be stored in Parquet files. 
Each parquet file will hold information for one partition,
i.e. combination of storage plugin, workspace, table name and metadata key.
Parquet files name will be based on `UUID` to ensure uniqueness.

Iceberg also supports data storage in Avro and ORC files, writing Drill metadata
in these formats can be added later.

## Metastore Operations flow

Metastore main goal is to provide ability to read and modify metadata.

### Read

Metastore data is read using `IcebergGenerics#read`. Iceberg will automatically determine
format in which data is stored (three formats are supported Parquet, Avro, ORC).
Based on given filter and select columns list, data will be returned in 
`org.apache.iceberg.data.Record` format which will be transformed 
into the list of `MetadataUnit` and returned to the caller.

Data in Iceberg table will be partitioned by storage plugin, workspace,
table name and metadata key. To avoid scanning all data and improve performance,
these fields can be included into filter expression.

### Add

To add metadata to Iceberg table, caller provides list of `MetadataUnit` which
will be written into Parquet files (current default format) grouped by
storage plugin, workspace, table name and metadata key. Each group will be written
into separate Parquet file and stored in the Drill table location based on
storage plugin, workspace, table name combination.
Note: storage plugin, workspace, table name and metadata key must not be null.

For each created Parquet file, `IcebergOperation#Overwrite` will be created.
List of overwrite operations will be executed in one transaction.
Main goal of overwrite operation to add or replace pointer of the existing file
in Iceberg table's partition information.

If transaction was successful, Iceberg table generates new snapshot and updates
its own metadata.

Assume, caller wants to add metadata for Drill table `dfs.tmp.nation`.
Parquet files with metadata for this table will be stored in 
`[METASTORE_ROOT_DIRECTORY]/dfs/tmp/nation` folder.

If `dfs.tmp.nation` is un-partitioned, it's metadata will be stored in two
parquet files: one file with general table information, 
another file with default segment information. 
If `dfs.tmp.nation` is partitioned, it will have also one file with general
information and `N` files with top-level segments information. 

File with general table information will always have one row.
Number of rows for default or top-level segment file will depend on segments 
metadata. Each row corresponds to one metadata unit: segment, file,
row group or partition.

If `dfs.tmp.nation` table has one segment to which belongs one file 
with two row groups and one partition and no inner segments, 
this segment's file will have five rows: 
- one row with top-level segment metadata;
- one row with file metadata;
- two rows with row group metadata;
- one row with partition metadata.

Such storage model allows easily to overwrite or delete existing top-level segments
without necessity to we-write all table metadata when it was only partially changed.

When `dfs.tmp.nation` metadata is created, table metadata location will store two files.
File names are generated based on `UUID` to ensure uniqueness.

```
...\dfs\tmp\nation\282205e0-88f2-4df2-aa3c-90d26ae0ad28.parquet (general info file)
...\dfs\tmp\nation\e6b20998-d640-4e53-9ece-5a063e498e1a.parquet (default segment file)

```

Once overwrite operation is committed, Iceberg table will have two partitions:
`\dfs\tmp\nation\GENERAL_INFO` and `\dfs\tmp\nation\DEFAULT_SEGMENT`.
Each of these partitions will point to dedicated Parquet file.
If table is partitioned, instead of `DEFAULT_SEGMENT`, top-level
segment metadata key will be indicated. For example, `\dfs\tmp\nation\dir0`.

### Overwrite

Process of overwriting existing partitions is almost the same as adding new partition.
Assume, `dfs.tmp.nation` default segment metadata has changed: new file was added.
Caller passes updated default segment metadata to the Metastore.
First, new Parquet file for updated default segment metadata is created.
Once overwrite operation is committed, `\dfs\tmp\nation\DEFAULT_SEGMENT` partition
will point to Parquet file with updated metadata. 
Table metadata location will store three files:

```
...\dfs\tmp\nation\282205e0-88f2-4df2-aa3c-90d26ae0ad28.parquet (general info file)
...\dfs\tmp\nation\e6b20998-d640-4e53-9ece-5a063e498e1a.parquet (old segment file)
...\dfs\tmp\nation\4930061e-1c1d-4c8e-a19e-b7b9a5f5f246.parquet (new segment file)

```

### Delete

To delete data from Iceberg table, caller provides filter by which data will be deleted.
Data in Iceberg table is partitioned by storage plugin, workspace, table name and metadata key,
thus delete filter should include filter expressions based in these four partition fields.

Delete operation removes partitions from Iceberg table, it does not remove data files to which
these partitions were pointing.

If delete operation was successful, Iceberg table generates new snapshot and updates
its own metadata.

### Purge

Allows to delete all data from Iceberg table. During this operation Iceberg table
is not deleted, history of all operations and data files are preserved.

## Data cleanup

Iceberg table provides ability to remove outdated data files and snapshots 
when they are no longer needed. Such support in Drill Iceberg Metastore will be added later.
