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

class Filesystem:
    @classmethod
    def __new__(cls, *args, **kwargs):
        if cls is not Filesystem:
            return super(Filesystem, cls).__new__(cls, *args, **kwargs)
        driver_type = oci_config['driver']['type']
        if driver_type == 'zfs':
            from .zfs_filesystem import ZFSFilesystem
            return super(Filesystem, cls).__new__(ZFSFilesystem)
        raise OCIError('Unsupported driver type (%s)' % driver_type)

    def __init__(self, id, layer=None):
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, id))
        self.id = id
        self.layer = layer

    @property
    def digest(self):
        return id_to_digest(self.id)

    @property
    def path(self):
        return pathlib.Path(oci_config['global']['path'], 'filesystems', self.id)

