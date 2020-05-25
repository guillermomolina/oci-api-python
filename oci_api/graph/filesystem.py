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


import logging
import pathlib
from oci_api import oci_config, OCIError
from oci_api.util import id_to_digest

log = logging.getLogger(__name__)

def get_filesystem_class():
    driver_type = oci_config['driver']['type']
    if driver_type == 'zfs':
        from .zfs_filesystem import ZFSFilesystem
        return ZFSFilesystem
    raise OCIError('Unsupported driver type (%s)' % driver_type)


class Filesystem:
    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls is not Filesystem:
            return super(Filesystem, cls).__new__(cls, *args, **kwargs)
        return super(Filesystem, cls).__new__(get_filesystem_class())

    @classmethod
    def create(cls, layer):
        filesystem_class = get_filesystem_class()
        return filesystem_class.create(layer)

    def __init__(self, id, layer, container_id):
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, id))
        self.id = id
        self.layer = layer
        self.container_id = container_id

    def is_mounted(self):
        return self.container_id is not None