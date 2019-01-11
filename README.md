# voltdb-ram-size
A command-line tool for estimating RAM usage of VoltDB tables

## Purpose

To help with estimating and planning for RAM requirements by providing more a more precise measurement of the memory used by tables in VoltDB.

The documentation reference on estimating RAM sizing based on the size of rows in a table: https://docs.voltdb.com/PlanningGuide/ChapMemoryRecs.php#MemSizePlan

The VoltDB web interface also has a Sizing Worksheet tab that estimates min and max RAM requirements based on input row counts for each table. However, for tables that have a number of large variable-length fields, the minimum or maximum estimate may not be realistic, and they can differ in some cases by multiple orders of magnitude. For example, a VARCHAR(5000) field could use as little as 8 bytes for a null pointer, or over 20KB if fully populated with 5000 special UTF-8 characters that use 4 bytes each (not likely).

This tool uses the SystemCatalog output to get the names and types of all the columns for the tables in the database, and uses this to query the average size of the actual values stored in the table. The Min and Max estimates are still provided, but an additional "Actual" estimate is based on the average sizes of a sample of values found in the table. In order for this to be useful, some representative data needs to be loaded. For example, perhaps VARCHAR(5000) field is populated with values that average 100 bytes, then you might want to know how many bytes that would use so you can estimate the RAM needed if this table will have 50 million records, rather than only basing your estimates on the minimum and maximum possible sizes.

## Pre-requisites

This tool assumes you have VoltDB installed locally, and that it's bin subdirectory is in your PATH.

Your database can be localhost or a remote server, but it should have tables defined and some representative data loaded.

## Usage

Use the following command to connect to locahost on the default port and get a report on ALL of the tables. If you have a large complex schema, this may take a while. Also, the queries that measure the length of variable length columns can be slow, so this is not meant to be run in a live production environment.

    ./run_report.sh

You can pass in a "hostname" parameter if you are connecting to a remote server

    ./run_report.sh myserver01


You can also pass in a "table name" parameter, but since this is a simple tool, if you do, you must also pass in the hostname parameter first. This will then give you a report for just that table.

    ./run_report.sh localhost my_table

Example output:

```
Connected...

TABLE: CONTESTANTS (REPLICATED)
====================================================================================================
  Column Name                     Datatype             (bytes) Min          Actual             Max
  -----------                     --------                     ---          ------             ---
  CONTESTANT_NAME                 VARCHAR(50)                   48              72             280
  CONTESTANT_NUMBER               INTEGER                        4               4               4
----------------------------------------------------------------------------------------------------
  Per Row                                                       52              76             284

  Total Size                            Row Count         (MB) Min          Actual             Max
  ----------                            ---------              ---          ------             ---
  Logical                                       6             0.00            0.00            0.00
  Physical                                     24             0.00            0.00            0.01

  @Statistics                                                                 0.00
  @Statistics (allocated)                                                     8.00

INDEXES (bytes):
  VOLTDB_AUTOGEN_CONSTRAINT_IDX_PK_CONTESTANTS: 1916928
  TOTAL: 1916928

```

The Per Row Actual is probably the number you want to multiply by the expected number of rows. The actual row count is tabulated below and multiplied for you. The Logical row count and size is the number of records in the table, where the Physical row count and size is based on the number of redundant copies of the records physically exist on the VoltDB server based on k-factor and if the table is partitioned or replicated. (Note: This does not take into account v8.1 or later where each server keeps only one copy of replicated tables).

The @Statistics and @Statistics (allocated) values are collected from @Statistics TABLE, but these values are rough estimates that may not be very accurate.

The size of Indexes is not calculated base on formulas, but is shown in bytes from @Statistics INDEX, which may also not be very accurate, but is provided for convenience.

### Note
Even the calculated "Actual" values are still estimates. They do not take into account how many pages of RAM are used and any other overhead associated with the table. They are simply based on the estimation formulas provided in the Planning Guide, and this tool just automates the process of tabulating the sizes based on those formulas as a convenience.

Also, note that "Physical" size for replicated tables will be over-estimated for v8.1 and later where each server has only 1 copy of each replicated table. To correct for this, you can divide the Physical value by the value of the "sitesperhost" setting in your deployment.xml file.
