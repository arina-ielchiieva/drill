# Metastore

Metastore stores metadata information which can be used in Drill during queries execution for better query planning.
It contains metadata about Drill tables, including general information, as well as
information about table segments, files, row groups, partitions.

`MetadataUnit` is a generic representation of metastore metadata unit, 
suitable to any metastore metadata type (table, segment, file, row group, partition).
`BaseTableMetadata`, `SegmentMetadata`, `FileMetadata`, `RowGroupMetadata` and `PartitionMetadata`
classes which are located in `org.apache.drill.metastore.metadata` package, have methods 
to convert to / from `MetadataUnit`.

Full table metadata consists of two major concepts: general information and top-level segments metadata.
Table general information contains basic table information and corresponds to `BaseTableMetadata` class.

Table can be non-partitioned and partitioned. Non-partitioned tables, have only one top-level segment 
which is called default (`MetadataInfo#DEFAULT_SEGMENT_KEY`). Partitioned tables may have several top-level segments.
Each top-level segment can include metadata about inner segments, files, row groups and partitions.

Unique table identifier in Metastore is combination of storage plugin, workspace and table name.
Table metadata inside is grouped by top-level segments, unique identifier of the top-level segment and its metadata
is storage plugin, workspace, table name and metadata key.

### Configuration

All configuration properties should reside in `drill.metastore` namespace.

Default Metastore configuration is defined in `drill-metastore-default.conf` file.
It can be overridden in `drill-metastore-override.conf`. Distribution configuration can be
indicated in `drill-metastore-distrib.conf`. Metastore implementations configuration can be
indicated in `drill-metastore-module.conf`.

## Initialization

`MetastoreRegistry` is initialized during Drillbit start up and is accessible though `DrillbitContext`.
It lazily initializes Metastore implementation based
on class implementation config property `drill.metastore.implementation.class`.

Metastore implementation must implement `Metastore` interface and
have constructor which accepts `DrillConfig`.

Once Drill Metastore is initialized, `DrillMetastore#get` method returns fully functioning
Drill Metastore instance. Whether to return new instance of Drill Metastore each time 
or cached instance, will depend on Metastore implementation.

### Metastore Metadata

In order to provide Metastore metadata functionality `Metastore.Metadata` interface must be implemented.

#### Versioning

Metastore may or may not support versioning depending on the implementation.
`Metastore.Metadata#supportsVersioning` and `Metastore.Metadata#version` methods 
are used to indicate versioning support. If Metastore does not support versioning, 
`Metastore.Metadata#version` returns undefined version (`Metastore.Metadata#UNDEFINED`).
If Metastore supports versioning, it is assumed that version is changed each time
data in the Metastore is modified and remains the same during read operations.
Metastore version is used to determine if metadata has changed after last access of the Metastore.

#### Properties

Metastore may or may not support properties depending on the implementation.
If properties are supported, Metastore implementation returns map with properties names and values,
otherwise empty map is returned. `Metastore.Metadata#properties` is used to obtain properties information. 

### Filter expression

Metastore data can be read or deleted based on the filter expression.
All filter expressions implement `FilterExpression` interface. 
List of supported filter operators is indicated in `FilterExpression.Operator` enum.
When filter expression is provided in read or delete operation, it's up to Metastore
implementation to convert it into suitable representation for storage.
For convenience, `FilterExpression.Visitor` can be implemented to traverse filter expression.

Filter expression can be simple and contain only one condition:

```
FilterExpression storagePlugin = FilterExpression.equal("storagePlugin", "dfs");
FilterExpression workspaces = FilterExpression.in("workspace", "root", "tmp");

```

Or it can be complex and contain several conditions combined with `AND` or `OR` operators.

```
  FilterExpression filter = FilterExpression.and(
    FilterExpression.equal("storagePlugin", "dfs"),
    FilterExpression.in("workspace", "root", "tmp"));
  
  metastore.read()
    .filters(filter)
    .execute();
```

SQL-like equivalent for the above operation is:

```
  select * from METASTORE
  where storagePlugin = 'dfs'
  and workspace in ('root', 'tmp')
```

### Metastore Read

In order to provide read functionality `Metastore.Read` interface must be implemented.
`Metastore.Read#columns` allows to specify list of columns to be retrieved from the Metastore.
`Metastore.Read#filter` allows to specify filter expression by which data will be retrieved.
`Metastore.Read#build` executes read operation from the Metastore and returns the results.
Data is returned in a form of list of `MetadataUnit`, it is caller responsibility to transform received
data into suitable representation. It is expected, if no result is found, empty of list of `MetadataUnit` 
will be returned, not null instance.

To retrieve `lastModifiedTime` for all tables in the `dfs` storage plugin the following code can be used:

```
  List<MetadataUnits> units = metastore.read()
    .columns("tableName", "lastModifiedTime")
    .filter(FilterExpression.equal("storagePlugin", "dfs")
    .execute();
```

### Metastore Basic Requests

`Metastore#basicRequests` provides list of most frequent requests to the Metastore without need 
to write filters and transformers from `MetadataUnit` class.

Assume caller needs to obtain general metadata about `dfs.tmp.nation` table: 

```
    TableInfo tableInfo = TableInfo.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .name("nation")
      .build();

    BaseTableMetadata metastoreTableInfo = metastore.basicRequests()
      .tableMetadata(tableInfo);
```

### Metastore Modify

In order to provide modify functionality `Metastore.Modify` interface must be implemented.

If Metastore implementation supports versioning, it is assumed that each modify operation will
change Metastore version.

#### Overwrite

`Metastore.Read#overwrite` writes data into Metastore or overwrites existing data by unique 
table metadata keys combination: storage plugin, workspace, table name and metadata key.
Caller provides only list of `MetadataUnit` to be written and Metastore implementation will decide 
how data will be stored or overwritten.

##### Adding new table metadata

Assume there is non-partitioned table which metadata is represented with two units.

1. Unit with table general information:

```
    MetadataUnit tableUnit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .metadataType(MetadataType.TABLE.name())
      ...
      .build();
```

2. Unit with default segment information with one file:

```
    MetadataUnit segmentUnit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .metadataType(MetadataType.SEGMENT.name())
      ...
      .build();
      
    MetadataUnit fileUnit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .metadataType(MetadataType.FILE.name())
      ...
      .build();
```

To add this table metadata in the Metastore, the following code can be executed:

```
    metastore.modify()
      .overwrite(tableUnit, segmentUnit)
      .execute();
```

##### Overwriting table metadata

Metastore allows only to overwrite metadata by unique combination of table identifier 
or table metadata identifiers (general info or top-level segments).

When only general table information has changed but segments default metadata did not change,
it is enough to overwrite only general information.

```
    MetadataUnit updatedTableUnit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.GENERAL_INFO_KEY)
      .metadataType(MetadataType.TABLE.name())
      ...
      .build();
      
    metastore.modify()
      .overwrite(updatedTableUnit)
      .execute();
```

If segment metadata was changed, new file was added to the default segment, 
all segment information must be overwritten.

```   
    MetadataUnit segmentUnit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .metadataType(MetadataType.SEGMENT.name())
      ...
      .build();
      
    MetadataUnit initialFileUnit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .metadataType(MetadataType.FILE.name())
      ...
      .build();
      
    MetadataUnit newFileUnit = MetadataUnit.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .tableName("nation")
      .metadataKey(MetadataInfo.DEFAULT_SEGMENT_KEY)
      .metadataType(MetadataType.FILE.name())
      ...
      .build();  
      
    metastore.modify()
      .overwrite(segmentUnit, initialFileUnit, newFileUnit)
      .execute();
```

#### Delete

`Metastore.Read#delete` deletes data from the Metastore based on the provided filter expression.

Assume metadata for table `dfs.tmp.nation` already exists in the Metastore and caller needed to delete it.
First, deletion filter must be created:

```
    FilterExpression filter = FilterExpression.and(
      FilterExpression.equal("storagePlugin", "dfs"),
      FilterExpression.equal("workspace", "tmp"),
      FilterExpression.equal("tableName", "nation"));
```

Such filter can be also generated using `TableInfo` class:

```
    TableInfo tableInfo = TableInfo.builder()
      .storagePlugin("dfs")
      .workspace("tmp")
      .name("nation")
      .build();
      
    FilterExpression filter = tableInfo.toFilter();  
```

Delete operation can be executed using the following code:

```
    metastore.modify()
      .delete(filter)
      .execute();
```

#### Purge

`Metastore.Read#purge` deletes all data from the Metastore.

```
    metastore.modify()
      .purge()
      .execute();
```

#### Transactions

Metastore implementation may or may not support transactions. If transactions are supported,
all operations in one `Metastore.Modify` instance will be executed fully or not executed at all.
If Metastore implementation does not support transactions, all operations will be executed consequently.

```
    metastore.modify()
      .overwrite(tableUnit1, segmentUnit1)
      .overwrite(tableUnit2, segmentUnit2)
      .delete(table3Filter)
      .delete(table4Filter)
      .execute();
```
