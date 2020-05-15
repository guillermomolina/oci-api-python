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

from .exceptions import OCIException, OCIError

oci_config = {
    'global': {
        'path': '/var/lib/oci',
        'run_path': '/var/run/oci'
    },
    'graph': {
        'driver': 'zfs',
        'zfs': {
            'filesystem': 'rpool/oci',
            'compression': 'lz4',
#            'compression': 'off',
        }
    }
}
