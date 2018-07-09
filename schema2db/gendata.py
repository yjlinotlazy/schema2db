"""
Generate data
"""
import pandas as pd
import numpy as np
import argparse
import os
import schema2db.randomdata as rd
from schema2db.parse_schema import SchemaParser


class DBGenerator():
    def __init__(self, schema):
        self.schema = schema
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

    def gen_db_data(self):
        """Top level method that generates a database that complies with
        the schema
        """
        self.reset_db()
        create = [p for p in self.schema.get('create')]
        waiting_room = create

        processed = []

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
        kwargs = {}
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

    parser = SchemaParser()
    parsed = parser.extract_sql_doc(args.schema_file)

    db_gen = DBGenerator(parsed)
    db_gen.gen_db_data()
    db_gen.export_db(args.destination)
