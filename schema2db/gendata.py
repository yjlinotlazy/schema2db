"""
Generate data
"""
import os
import argparse
import pandas as pd
import numpy as np
import schema2db.randomdata as rd
from schema2db.parse_schema import SchemaParser


class DBGenerator():
    def __init__(self, schema):
        """schema can either be a processed schema as dict,
        or a string pointing to the path of the schema sql file
        """
        if isinstance(schema, dict):
            self.schema = schema
        elif isinstance(schema, str):
             parser = SchemaParser()
             self.schema = parser.extract_sql_doc(schema)
        else:
            raise ValueError("Unsupported input type {}".format(type(schema)))
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

    def gen_db_data(self, preload={}):
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
                    constraints = self.schema.get('alter').get(cand, {})
                    self.db[cand] = self.gen_table(table_args, constraints)
                    processed.append(cand)
                    end_count -= 1

            if start_count == end_count:
                raise ValueError('''There are some circular dependencies in foreign keys.
                Please double check your constraints''')

    def gen_table(self, create_sql, constrain_sql, row_num=50):
        """Generates a single table"""
        enums = {}
        foreign_keys = {}
        rows = row_num
        table = None
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
                rows = min(len(choices), rows)
            elif foreign_keys.get(name):
                d = foreign_keys.get(name)[0]
                choices = self.db[d['referenced']][d['source_column']].tolist()
                kwargs['choices'] = choices
                rows = min(len(choices), rows)
            rowdata = self.gen_column_data(**kwargs)
            if table is None:
                table = pd.DataFrame({name: rowdata})
            else:
                table[name] = rowdata
        return table

    @staticmethod
    def gen_column_data(datatype='int', args=None, choices=None,
                        signed=False,
                        primary_key=False, isnull=True,
                        num_rows=50):
        if choices:
            if primary_key:
                return np.random.choice(choices, num_rows, replace=False)
            return np.random.choice(choices, num_rows, replace=True)
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
            return list(set(datalist))[:min(num_rows, len(set(datalist)))]
        return datalist

    """==============Utilities to turn csv into inserts======"""
    def sql_value(self, a, type_a='varchar'):
        if type_a.lower() in ['int', 'decimal']:
            return str(a)
        elif type_a.lower() == 'varchar':
            return "'{}'".format(a)
        elif type_a.lower() in ['date', 'datetime']:
            return "'{}'".format(a)
        return str(a)

    def to_insert(self, tablename, types, x):
        total_cols = [t for t in types]
        valid_cols = [c for c in total_cols if x[c]]
        columns_sql = ','.join([self.sql_value(v) for v in valid_cols])
        values = ','.join([self.sql_value(x[v], types[v]) for v in valid_cols])
        statement = 'INSERT INTO {} ({}) VALUES ({})'.format(tablename,
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
                f.write('\n')


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
