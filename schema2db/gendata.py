"""
Generate data based on database schema written in sql format
"""
import os
import argparse
import pandas as pd
import numpy as np
import schema2db.randomdata as rd
from schema2db.parse_schema import SchemaParser


class DBGenerator():
    def __init__(self, schema, exclusive_list=None, exclude_on=None):
        """
        Parameters
        ----------
        schema : dict or str
        exclusive_list
        exclude_on
        """
        if isinstance(schema, dict):
            self.schema = schema
        elif isinstance(schema, str):
            parser = SchemaParser()
            self.schema = parser.extract_sql_doc(schema)
        else:
            raise ValueError("Unsupported input type {}".format(type(schema)))

        if exclusive_list:
            if exclude_on:
                self.parse_exclusive_tables(exclusive_list, exclude_on)
            else:
                raise ValueError("You must specify the columns to be mutually exclusive")
        self.db = {}

    def get_db(self):
        """Return generated database"""
        return self.db

    def reset_db(self):
        self.db = {}

    def export_db(self, outpath):
        for tablename in self.db:
            self.db[tablename].to_csv(os.path.join(outpath, "{}.csv".format(tablename)),
                                      index=False)

    def gen_db_data(self, preload={}, row_num=100):
        """Top level method that generates a database that complies with
        the schema
        preload: preload some tables from csv files or dataframe
        """
        self.reset_db()
        for p in preload:
            entry = preload[p]
            if isinstance(entry, str):
                self.db[p] = pd.read_csv(entry)
            elif isinstance(entry, pd.DataFrame):
                self.db[p] = entry
            else:
                raise ValueError("Unknown preloaded data type")
        processed = [p for p in preload]
        create = [p for p in self.schema.get('create') if p not in preload]
        waiting_room = create

        while waiting_room:
            start_count = len(waiting_room)
            end_count = start_count
            for i in range(start_count):
                cand = waiting_room.pop()
                valid = True
                foreign_tables = [ref['referenced']
                                  for ref in self.schema.get('alter')
                                  .get(cand, {})
                                  .get('foreign_keys', [])]
                for f in foreign_tables:
                    if f not in processed:
                        waiting_room.insert(0, cand)
                        valid = False
                        break
                if valid:
                    table_args = self.schema['create'][cand]
                    constraints = self.schema.get('alter', {}).get(cand, {})
                    exclusive = self.schema.get('exclusive', {}).get(cand, {})
                    self.db[cand] = self.gen_table(table_args,
                                                   constraints,
                                                   exclusive,
                                                   row_num=row_num)
                    processed.append(cand)
                    end_count -= 1

            if start_count == end_count:
                raise ValueError('''There are some circular dependencies in foreign keys.
                Please double check your constraints''')

    def gen_table(self, create_sql, constrain_sql={},
                  exclusive_sql={}, row_num=50):
        """Generates a single table"""
        enums = {}
        foreign_keys = {}
        rows = row_num
        tabledata = None
        for c in constrain_sql.get('check', []):
            if c['type'] == 'enum':
                if c['column'] in enums:
                    enums[c['column']].append(c)
                else:
                    enums[c['column']] = [c]
        for c in constrain_sql.get('foreign_keys', []):
            if c['column'] in foreign_keys:
                foreign_keys[c['column']].append(c)
            else:
                foreign_keys[c['column']] = [c]

        exclusive_tables = exclusive_sql.get('tables', [])
        exclusive_columns = exclusive_sql.get('columns', [])
        for col_sql in create_sql['columns']:
            kwargs = {}
            name = col_sql['name']
            kwargs['datatype'] = col_sql['type']['type']
            kwargs['args'] = [int(arg) for arg in col_sql['type']['args']]
            kwargs['signed'] = col_sql['type'].get('signed')
            kwargs['isnull'] = col_sql.get('null')
            kwargs['primary_key'] = name in create_sql['primary_keys']

            if enums.get(name):
                choices = enums.get(name)[0]['values']
                kwargs['choices'] = choices
            elif foreign_keys.get(name):
                d = foreign_keys.get(name)[0]
                choices = self.db[d['referenced']][d['source_column']].tolist()
                kwargs['choices'] = choices
            # if this column is subject to exclusion constraints, do something
            if name in exclusive_columns:
                excluded_values = []
                unprocessed = 1

                for table in exclusive_tables:
                    df = self.db.get(table)
                    if df is not None:
                        values = df[name].tolist()
                        excluded_values.extend(values)
                    else:
                        unprocessed += 1
                if 'choices' in kwargs:
                    kwargs['choices'] = [c for c in kwargs['choices'] if c not in excluded_values]
                    if unprocessed > 0:
                        # if there are unprocessed tables left, they will share the same choice pool
                        sample_size = int(len(kwargs['choices']) / unprocessed)
                        kwargs['choices'] = np.random.choice(kwargs['choices'],
                                                             sample_size,
                                                             replace=False)
                else:
                    kwargs['excluded'] = excluded_values

            column_data = self.gen_column_data(num_rows=rows, **kwargs)
            if tabledata is None:
                tabledata = pd.DataFrame({name: column_data})
            else:
                # trim table if the column to be inserted is shorter than
                # table height
                if len(column_data) < tabledata.shape[0]:
                    tabledata = tabledata.head(len(column_data))
                if len(column_data) > tabledata.shape[0]:
                    tabledata[name] = column_data[:tabledata.shape[0]]
                else:
                    tabledata[name] = column_data
        return tabledata

    @staticmethod
    def gen_column_data(datatype='int', args=None, choices=None,
                        signed=False,
                        primary_key=False, isnull=True,
                        excluded=[],
                        num_rows=50):
        """
        Parameters
        ----------
        excluded: list
            values that should be excluded from the column
        """

        if choices is not None:
            if len(choices) == 0:
                raise ValueError('No value to choose from!')
            choices_mod = [c for c in choices if c not in excluded]
            if primary_key:
                # if primary key, then there can be no more rows than the number
                # of choices
                num_rows = min(num_rows, len(choices_mod))
                selected = np.random.choice(choices_mod, num_rows, replace=False)
                return selected
            return np.random.choice(choices_mod, num_rows, replace=True)
        else:
            if primary_key:
                rows = 2 * num_rows
            else:
                rows = num_rows
            datalist = rd.random_list(datatype=datatype, args=args,
                                      signed=signed, length=rows)
        if isnull:
            datalist = [rd.gen_null(x) for x in datalist]
        if primary_key:
            # remove spaces if the primary key is of type varchar
            if datatype == 'varchar':
                datalist = [''.join(k.split()) for k in datalist]
            datalist = list(set(datalist))[:min(num_rows, len(set(datalist)))]
        datalist = [d for d in datalist if d not in excluded]
        return datalist

    def parse_exclusive_tables(self, exclusive_list, exclude_on):
        """This adds a special dict to guarantee that multiple tables have foreign
        keys that are mutually exclusive.
        Example: in the example below, the aim is that a userid can show up in only
        one of the two tables, but not both
        Individual table
        userid | person_name
          1    |     anne
          2    |     peter

        Company table
        userid | company_name
          1    |    company1
          2    |    company2

        suppose the exclusive list is handled properly, instead of above tables, we
        should have
        userid | person_name
          1    |     anne

        userid | company_name
          2    |    company

        The choice of which rows stay in which table is arbitrary
        """

        self.schema['exclusive'] = {}
        if isinstance(exclude_on, str):
            exclude_on = [exclude_on]
        for e in exclusive_list:
            self.schema['exclusive'][e] = {'tables': [ee for ee in exclusive_list if ee != e],
                                           'columns': exclude_on}

    # ============== Utilities to turn csv into inserts ====== #
    @staticmethod
    def sql_value(a, type_a='none'):
        if type_a.lower() in ['int', 'decimal']:
            return str(a)
        elif type_a.lower() == 'varchar':
            return "'{}'".format(a)
        elif type_a.lower() in ['date', 'datetime']:
            return "'{}'".format(a)
        return str(a)

    def to_insert(self, table_name, types, x):
        total_cols = [t for t in types]
        valid_cols = [c for c in total_cols if x[c] or x[c] == 0]
        columns_sql = ','.join([self.sql_value(v) for v in valid_cols])
        values = ','.join([self.sql_value(x[v], types[v]) for v in valid_cols])
        statement = 'INSERT INTO {} ({}) VALUES ({})'.format(table_name,
                                                             columns_sql,
                                                             values)
        return statement

    def table_to_inserts(self, df, tablename, col_types, path=None):
        dff = df.copy()
        printed = dff.apply(lambda x: self.to_insert(tablename,
                                                     col_types, x),
                            axis=1).tolist()
        if not path:
            path = tablename + '.sql'
        with open(path, 'w') as f:
            for p in printed:
                f.write(p)
                f.write(';\n')

    def db_to_inserts(self, path=''):
        for tablename in self.db:
            columns = self.schema['create'][tablename]['columns']
            types = {i['name']:i['type']['type'] for i in columns}
            self.table_to_inserts(self.db[tablename],
                                  tablename,
                                  types,
                                  os.path.join(path, tablename + '.sql'))


def main():
    epi = """Usage: schema2dbdata <schema.sql> <output folder>
    """
    parser = argparse.ArgumentParser(description='A simple tool to generate data from sql commands',
                                     epilog=epi)
    parser.add_argument('schema_file', type=str,
                        help='path to the schema file')
    parser.add_argument('destination', type=str,
                        help='destination folder of the database csv files')

    args = parser.parse_args()

    db_gen = DBGenerator(args.schema_file)
    db_gen.gen_db_data()
    db_gen.export_db(args.destination)
