# Introduction
This simply package generates random data based on sql schema file. It
allows basic operations to create tables and add some
constraints. Although the data it generates does not look like real
world data, it can be used as test cases for building database tools.

# Installation

```bash
git clone https://github.com/yjlinotlazy/schema2db.git
cd schema2db
pip install -e .
```

# Usage

To use in command line:
```bash
schema2dbdata <input.sql> <outputfolder/>
```

To use in a python script:
```python

```

## Input file format

The schema file should consist of blocks of SQL commands for 2
operations, `create` and `alter` (optional). It should look like

```sql
create table table1 (
   ...
);

alter table table1 (
   ...
);
```

## Examples

[Example input file](tests/testdata/testschema1.sql)

# Known Issues
