# Tsurugi DB input plugin for Embulk

Tsurugi DB input plugin for Embulk loads records from Tsurugi using [Tsubakuro](https://github.com/project-tsurugi/tsubakuro).

## Overview

* **Plugin type**: input
* Embulk 0.10 or later
* Java11 or later

## Example

### query

```yaml
in:
  type: tsurugidb
  endpoint: tcp://localhost:12345
  query: select * from test order by pk;
```


### table

```yaml
in:
  type: tsurugidb
  endpoint: tcp://localhost:12345
  table: test
  order_by: pk
```




## Configuration

* **endpoint**: endpoint for Tsurugi (string, required)
* **connection_label**: connection label (string, default: `embulk-input-tsurugidb`)
* **method**: (string, default: `select`)
* **tx_type**: transaction type (`OCC`, `LTX`, `RTX`) (string, default: `RTX`)
* **tx_label**: transaction label (string, default: `embulk-input-tsurugidb`)
* **tx_write_preserve**: (LTX only) write preserve (list of string, defualt: empty list)
* **tx_inclusive_read_area**: (LTX only) inclusive read area (list of string, defualt: empty list)
* **tx_exclusive_read_area**: (LTX only) exclusive read area (list of string, defualt: empty list)
* **tx_priority**: (LTX, RTX only) transaction priority (string, default: `null`)
* **commit_type**: commit type (string, default: `default`)
* **session_keep_alive**: session shutdown type (boolean, default: `null`)
* **session_shutdown_type**: session shutdown type (string, default: `nothing`)
* if you write SQL directly:
  * **query**: SQL to run (string)
* if **query** is not set,
  * **table**: destination table name (string, required)
  * **select**: expression of select (string, default: `*`)
  * **where**: WHERE condition to filter the rows (string, default: no-condition)
  * **order_by**: expression of ORDER BY to sort rows (string, default: not sorted)
* **default_timezone**:
* **column_options**: advanced: key-value pairs where key is a column name and value is options for the column.
  * **value_type**: ignored
  * **type**:  Column values are converted to this embulk type. Available values options are: `boolean`, `long`, `double`, `string`, `json`, `timestamp`).
  * **timestamp_format**:
  * **timezone**:
* **connect_timeout**: timeout for establishment of a database connection (integer (seconds), default: 300)
* **begin_timeout**: timeout for begin transaction (integer (seconds), default: 300)
* **select_timeout**: timeout for select (integer (seconds), default: 300)
* **commit_timeout**: timeout for commit (integer (seconds), default: 300)
* **session_shutdown_timeout**: timeout for session shutdown (integer (seconds), default: 300)


## Install

1. install plugin
   ```
   $ java -jar embulk-0.11.3.jar install io.github.hishidama.embulk:embulk-input-tsurugidb:1.5.0
   ```

2. add setting to $HOME/.embulk/embulk.properties
   ```
   plugins.input.tsurugidb=maven:io.github.hishidama.embulk:tsurugidb:1.5.0
   ```

| version       | Tsurugi       | Tsubakuro |
|---------------|---------------|-----------|
| 1.0.0         | 1.0.0         | 1.6.0     |
| 1.1.0         | 1.1.0 - 1.2.0 | 1.7.0     |
| 1.3.0         | 1.3.0         | 1.8.0     |
| 1.4.0         | 1.4.0         | 1.9.0     |
| 1.5.0         | 1.5.0         | 1.10.0    |


## Build

### Test

```
./gradlew test -Pendpoint=tcp://localhost:12345
ls build/reports/tests/test/
```

### Build to local Maven repository

```
./gradlew generatePomFileForMavenJavaPublication
mvn install -f build/publications/mavenJava/pom-default.xml
./gradlew publishToMavenLocal
```

