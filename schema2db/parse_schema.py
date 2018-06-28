import re

class SchemaParser():
    def __init__(self):
        pass

    def extract_sql_doc(self, inputfile):
        """ Extract sql components from the entire document
        params:
           inputfile: str, name of the input sql file
        """
        with open(inputfile, 'r') as f:
            inputstr = f.read()
        blocks = self._clean_doc(inputstr).split(";\n")
        components = []
        for b in blocks:
            components.append(self.extract_sql_block(b))
        return components

    def extract_sql_block(self, block):
        """ Parse a block of sql commands and return corresponding
        sql components
        """
        operation = block.split()[0].lower()
        if operation == 'create':
            return self.parse_create_block(block)
        elif operation == 'alter':
            return self.parse_alter_block(block)
        else:
            raise NotImplementedError("{} operation not supported".format(operation))

    def parse_create_block(self, sql_str):
        lines = sql_str.split('\n')
        table = {'columns': [], 'primary_keys': []}
        table['tablename'] = self._get_table_name(lines[0])
        for l in lines[1:]:
            if 'primary key' in l.lower():
                table['primary_keys'].append(self._parse_keys(l))
            elif re.findall("[a-zA-Z]+", l):
                table['columns'].append(self._parse_items(l))
        return table

    def parse_alter_block(self, sql_cmds):
        constraints = ["constraints placeholder"]
        return constraints

    def _clean_doc(self, doc):
        """clean out extra spaces
        """
        lines = doc.split('\n')
        lines = [" ".join(l.split()) for l in lines if l and not l.isspace()]
        return '\n'.join(lines)

    def _get_table_name(self, line):
        words = line.split()
        if words[1].lower() != 'table'.lower():
            raise ValueError("You can only create tables, not {}".format(words[1]))
        else:
            return words[2]

    def _extract_datatype(self, words):
        strtype = {}
        next_pos = 1
        strtype['type'] = (re.findall("[a-zA-Z]+", words[0]))[0].lower()
        strtype['args'] = re.findall("[0-9]+", words[0])
        if len(words) > 1 and words[1].lower() == 'signed':
            strtype['signed'] = True
            next_pos += 1
        elif len(words) > 1 and words[1].lower() == 'unsigned':
            strtype['signed'] = False
            next_pos += 1
        return strtype, words[next_pos:]

    def _extract_null(self, words):
        ifnull = None
        if 'not null' in " ".join([s.lower() for s in words]):
            ifnull = False
        elif 'null' in " ".join([s.lower() for s in words]):
            ifnull = True
        return ifnull

    def _extract_default(self, words):
        lower_cased = [w.lower() for w in words]
        if 'default' in lower_cased:
            idx = lower_cased.index('default')
            if len(words) > idx + 1:
                return words[idx+1]
        return None

    def _parse_items(self, l):
        c = l.split()
        cmds = []
        cmds = {'name': c[0]}
        cmds['type'], cmd_remaining = self._extract_datatype(c[1:])
        cmds['null'] = self._extract_null(cmd_remaining)
        default_value = self._extract_default(cmd_remaining)
        if default_value:
            cmds['default'] = default_value
        return cmds

    def _parse_keys(self, l):
        tokens = re.findall("[a-zA-Z]+", l)
        return [t for t in tokens if t.lower() not in('primary', 'key')]
