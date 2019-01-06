# Introduction
This simple package generates random data based on sql schema file. It
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

To use in a python script, see example in [[demo/demo.ipynb]]

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

The main issue is that...this package was developed in a hurry to be
used in time for a database course. It was not well commented and
tested. I _think_ there's a major bug but I don't remember what it
is. Due to limited usage and low generalizability, I decide not to
invest more.
