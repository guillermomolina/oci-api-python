# Copyright 2020, Guillermo Adrián Molina
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

 
def print_table(table):
    if len(table) == 0:
        return

    columns = []
    # initialize columns from first row's keys
    for key in table[0]:
        if key != 'ociVersion':             
            columns.append({
                'key': key,
                'tittle': key.upper(),
                'length': len(key)
            })

    # adjust columns lenghts to max record sizes
    for column in columns:
        for row in table:
            value = str(row[column['key']]).replace('\t', ' ')
            row[column['key']] = value
            column['length'] = max(column['length'], len(value))


    # print headers
    strings = []
    for column in columns:
        str_format = '{:%s}' % str(column['length'])
        strings.append(str_format.format(column['tittle']))
    print('   '.join(strings))

    for row in table:
        strings = []
        for column in columns:
            str_format = '{:%s}' % str(column['length'])
            value = row[column['key']]
            strings.append(str_format.format(value))
        print('   '.join(strings))
