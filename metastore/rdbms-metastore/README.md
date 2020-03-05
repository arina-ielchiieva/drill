# RDBMS Metastore

RDBMS Metastore implementation allows to store Drill Metastore metadata in configured RDBMS.

## Configuration

Currently, RDBMS Metastore is not default Drill Metastore implementation.
To enable RDBMS Metastore create `drill-metastore-override.conf` and indicate RDBMS Metastore class:

```
drill.metastore: {
  implementation.class: "org.apache.drill.metastore.rdbms.RdbmsMetastore"
}
```

### Connection properties

Data source connection properties allows to indicate how to connect to Drill Metastore database.

`drill.metastore.rdbms.data_source.driver` - driver class name. Required. 
Note, driver must be included into Drill classpath prior to start up for all databases except of SQLite.

`drill.metastore.rdbms.data_source.url` - connection url. Required.

`drill.metastore.rdbms.data_source.username` - database user on whose behalf the connection is
being made. Optional, if database does not require user to connect. 

`drill.metastore.rdbms.data_source.password` - database user's password. 
Optional, if database does not require user's password to connect.

`drill.metastore.rdbms.data_source.properties` - specifies properties which will be used
during data source creation. See list of available [Hikari properties](https://github.com/brettwooldridge/HikariCP)
for more details.

### Default configuration 

Out of the box, Drill RDBMS Metastore is configured to use embedded file system based SQLite database.
It will be created locally in user's home directory under `${drill.exec.zk.root}"/metastore` location.

Default setup can be used only in Drill embedded mode. 
If SQLite setup will be used in distributed mode, each drillbit will have it's own SQLite instance
which will lead to bogus results during queries execution.
In distributed mode, database instance must be accessible for all drillbits.

### Custom configuration

`drill-metastore-override.conf` is used to customize connection details to the Drill Metastore database.
See `drill-metastore-override-example.conf` for more details.

#### Example of PostgreSQL configuration

```
drill.metastore: {
  implementation.class: "org.apache.drill.metastore.rdbms.RdbmsMetastore",
  rdbms: {
    data_source: {
      driver: "org.postgresql.Driver",
      url: "jdbc:postgresql://localhost:1234/mydb?currentSchema=drill_metastore",
      username: "user",
      password: "password"
    }
  }
}
```

Note: PostgreSQL JDBC driver must be present in Drill classpath.

#### Example of MySQL configuration

```
drill.metastore: {
  implementation.class: "org.apache.drill.metastore.rdbms.RdbmsMetastore",
  rdbms: {
    data_source: {
      driver: "com.mysql.cj.jdbc.Driver",
      url: "jdbc:mysql://localhost:1234/drill_metastore",
      username: "user",
      password: "password"
    }
  }
}
```

Note: MySQL JDBC driver must be present in Drill classpath.

##### Driver version

For MySQL connector version 6+, use `com.mysql.cj.jdbc.Driver` driver class,
for older versions use `com.mysql.jdbc.Driver`.

## Tables structure

Drill Metastore consists of components. Currently, only `tables` component is implemented.
This component provides metadata about Drill tables, including their segments, files, row groups and partitions.
In Drill `tables` component unit is represented by `TableMetadataUnit` class which is applicable to any metadata type.
Fields which are not applicable to particular metadata type, remain unset.

In RDBMS Drill Metastore each `tables` component metadata type has it's own table.
There are five tables: `TABLES`, `SEGMENTS`, `FILES`, `ROW_GROUPS`, `PARTITIONS`.
These tables structure and primary keys are defined based on fields specific for each metadata type.
See `src/main/resources/db/changelog/changes/initial_ddls.yaml` for more details.

### Tables creation

RDBMS Metastore is using [Liquibase](https://www.liquibase.org/documentation/core-concepts/index.html)
to create all needed tables during RDBMS Metastore initialization, users should not create any tables manually.

### Database schema

Liquibase is using yaml configuration file to apply changes into database schema: `src/main/resources/db/changelog/chnagelog.yaml`.
It converts yaml content into DDL / DML commands suitable to database syntax based on connection details.
See list of supported databases: https://www.liquibase.org/databases.html.

All Drill Metastore tables will be created in database default schema, unless schema is set in the connection url. 
It's recommended prior to using RDBMS Metastore to create dedicated Drill Metastore schema, for example, `drill_metastore`
and indicate it in the connection url.

Example:

PostgreSQL: `jdbc:postgresql://localhost:1234/mydb?currentSchema=drill_metastore`

MySQL: `jdbc:mysql://localhost:1234/drill_metastore`

Note: database user must have not only read / write permission in Drill Metastore schema 
but also permission to create / modify database objects.

### Liquibase tables

During Drill RDBMS Metastore initialization, Liquibase will create two internal tracking tables:
`DATABASECHANGELOG` and `DATABASECHANGELOGLOCK`. They are needed to track schema changes and concurrent updates.
See https://www.liquibase.org/get_started/how-lb-works.html for more details.

## Query execution

SQL queries issued to RDBMS Metastore tables are generated using [JOOQ](https://www.jooq.org/doc/3.13/manual/getting-started/).
JOOQ allows programmatically construct SQL queries at runtime and provides executor to run generated queries.

JOOQ generates SQL statements based on SQL dialect determined by database connection details.
List of supported dialects: https://www.jooq.org/javadoc/3.13.x/org.jooq/org/jooq/SQLDialect.html.
Note: dialects annotated with `@Pro` are not supported, since open-source version of JOOQ is used.
