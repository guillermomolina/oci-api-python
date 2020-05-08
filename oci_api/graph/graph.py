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

from oci_api import OCIError
from .zfs_driver import ZfsDriver

class Graph:
    @classmethod
    def driver(cls, driver_type='zfs'):
        if driver_type != 'zfs':
            raise OCIError('Unknown driver type (%s)' % driver_type)
        return ZfsDriver()