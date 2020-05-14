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
import shutil
from oci_api import OCIError, oci_config
from oci_api.util.zfs import zfs_create, zfs_get, zfs_set, zfs_snapshot, \
    zfs_send, zfs_destroy, zfs_is_snapshot, zfs_is_filesystem, zfs_list, zfs_clone
from oci_api.util.file import rm, untar, uncompress, du

log = logging.getLogger(__name__)

# ZFSLayer -> zfs snapshot
# ZFSFilesystem -> zfs filesystem

class ZFSFilesystem:
    def __init__(self, id=None, origin=None):
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, id or ''))
        self.id = id
        self.origin = origin

    @property
    def size(self):
        return du(self.path)

    @property
    def path(self):
        return pathlib.Path(oci_config['global']['path'], 'zfs', self.id)

    @property
    def zfs_filesystem(self):
        root_zfs_filesystem = oci_config['graph']['zfs']['filesystem']
        return '%s/%s' % (root_zfs_filesystem, self.id)

    @property
    def zfs_snapshot(self):
        return '%s@diff' % self.zfs_filesystem

    def zfs_size(self):
        # compressed and/or deduplicated size is smaller than actuall size
         return zfs_get(self.zfs_filesystem, 'used')

    def create(self):
        if self.origin is None:
            zfs_filesystem = zfs_create(self.zfs_filesystem, mountpoint=self.path)
            if zfs_filesystem != self.zfs_filesystem:
                raise OCIError('Could not create zfs filesystem (%s)' % self.zfs_filesystem)
        else:
            zfs_filesystem = zfs_clone(self.zfs_filesystem, self.origin.zfs_snapshot, 
                mountpoint=self.path)
            if zfs_filesystem != self.zfs_filesystem:
                raise OCIError('Could not clone zfs filesystem (%s) from zfs snapshot (%s)' % 
                    (self.zfs_filesystem, self.origin.zfs_snapshot))

    def destroy(self):
        if zfs_destroy(self.zfs_filesystem, recursive=True) != 0:
            raise OCIError('Could not destroy zfs filesystem (%s)' % self.zfs_filesystem)
        rm(self.path)

    def add_tar_file(self, tar_file_path, destination_path=None):
        local_destination_path = self.path
        if destination_path is not None:
            local_destination_path = local_destination_path.joinpath(destination_path).resolve()
        untar(local_destination_path, tar_file_path=tar_file_path)

    def commit(self):
        zfs_set(self.zfs_filesystem, readonly=True)
        zfs_snapshot('diff', self.zfs_filesystem)