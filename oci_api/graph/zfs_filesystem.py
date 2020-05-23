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
import tarfile
import tempfile
import humanize
from oci_api import OCIError, oci_config
from oci_api.util.zfs import zfs_create, zfs_get, zfs_set, zfs_snapshot, zfs_destroy, \
    zfs_clone, zfs_diff, zfs_is_filesystem, zfs_rename
from oci_api.util.file import rm, untar, uncompress, du, sha256sum
from .filesystem import Filesystem
from .exceptions import FilesystemInUseException

log = logging.getLogger(__name__)

def create_base_zfs():
    filesystems_path = pathlib.Path(oci_config['global']['path'], 'filesystems')
    if not filesystems_path.is_dir():
        filesystems_path.mkdir(parents=True)
    base_zfs = oci_config['driver']['zfs']['base']
    compression = oci_config['driver']['zfs']['compression']
    if not zfs_is_filesystem(base_zfs):
        log.info('Creating base zfs (%s)' % base_zfs)
        base_zfs = zfs_create(base_zfs, mountpoint=filesystems_path, compression=compression)
        if base_zfs is None:
            raise OCIError('Could not create base zfs (%s)' % base_zfs)

class ZFSFilesystem(Filesystem):
    @property
    def zfs_filesystem(self):
        base_zfs_filesystem = oci_config['driver']['zfs']['base']
        return '%s/%s' % (base_zfs_filesystem, self.id)

    @property
    def zfs_snapshot(self):
        return '%s@diff' % self.zfs_filesystem

    def size(self):
        return du(self.path)

    def virtual_size(self):
        # compressed and/or deduplicated size is smaller than actuall size
        return zfs_get(self.zfs_filesystem, 'used')

    '''def diff_size(self):
        log.debug('Start calculating diff size (%s)' % self.id)
        origin_snapshot = None
        if self.origin is not None:
            origin_snapshot = self.origin.zfs_snapshot
        path = self.path
        size = 0
        for change_info in zfs_diff(self.zfs_snapshot, origin_snapshot, include_file_types=True):
            change_type = change_info[0]
            file_type = change_info[1]
            if (change_type == 'M' or change_type == '+') and file_type == 'F':
                    file_path = pathlib.Path(change_info[2])
                    file_size = file_path.stat().st_size
                    #log.debug('File %s, size %d' % (file_path, file_size))
                    size += file_size
        log.debug('Finish calculating diff size (%s)' % self.id)
        return size'''

    def create(self):
        create_base_zfs()
        if self.layer is None:
            log.debug('Creating filesystem (%s)' % self.zfs_filesystem)
            zfs_filesystem = zfs_create(self.zfs_filesystem, mountpoint=self.path)
            if zfs_filesystem != self.zfs_filesystem:
                raise OCIError('Could not create zfs filesystem (%s)' % self.zfs_filesystem)
        else:
            origin = self.layer.filesystem
            log.debug('Cloning filesystem (%s) from (%s)' 
                % (self.zfs_filesystem, origin.zfs_snapshot))
            zfs_filesystem = zfs_clone(self.zfs_filesystem, origin.zfs_snapshot, 
                mountpoint=self.path)
            if zfs_filesystem != self.zfs_filesystem:
                raise OCIError('Could not clone zfs filesystem (%s) from zfs snapshot (%s)' % 
                    (self.zfs_filesystem, origin.zfs_snapshot))

    def destroy(self):
        if zfs_destroy(self.zfs_filesystem, recursive=True) != 0:
            raise OCIError('Could not destroy zfs filesystem (%s)' % self.zfs_filesystem)
        rm(self.path)

    def commit(self, changeset_file_path):
        zfs_set(self.zfs_filesystem, readonly=True)
        zfs_snapshot('diff', self.zfs_filesystem)
        self.save_changeset(changeset_file_path)
        diff_id = sha256sum(changeset_file_path)
        if diff_id is None:
            raise OCIError('Could not get hash of file (%s)' % str(changeset_file_path))

        new_zfs_filesystem = oci_config['driver']['zfs']['base'] + '/' + diff_id
        if zfs_is_filesystem(new_zfs_filesystem):
            raise FilesystemInUseException(diff_id)

        previous_zfs_filesystem = self.zfs_filesystem
        previous_path = self.path
        self.id = diff_id
        zfs_rename(previous_zfs_filesystem, self.zfs_filesystem)
        zfs_set(self.zfs_filesystem, mountpoint=self.path)
        rm(previous_path)
    
    def load_changeset(self, changeset_file_path):
        log.debug('Start loading changeset (%s)' % str(changeset_file_path))
        path = self.path
        size = 0
        with tarfile.open(changeset_file_path, "r") as tar_file:
            for member in tar_file:
                file_path = pathlib.Path(member.name)
                if file_path.name.startswith('.wh.'):
                    file_path = path.joinpath(file_path)
                    if file_path.name == '.wh..wh..opq':
                        rm(file_path.parent, recursive=True)
                    else:
                        file_path = file_path.parent.joinpath(file_path.name[4:])
                        rm(file_path)
                else:
                    size += member.size
                    tar_file.extract(member, path)
        log.debug('Finish loading changeset (%s), size: %s' % 
            (str(changeset_file_path), humanize.naturalsize(size)))
    
    def save_changeset(self, changeset_file_path):
        log.debug('Start saving changeset (%s)' % str(changeset_file_path))
        origin_snapshot = None
        if self.layer is not None:
            origin_snapshot = self.layer.filesystem.zfs_snapshot
        with tarfile.open(changeset_file_path, "w") as tar_file:
            with tempfile.NamedTemporaryFile() as wh_temp_file:
                path = self.path
                for change_info in zfs_diff(self.zfs_snapshot, origin_snapshot):
                    change_type = change_info[0]
                    file_path = pathlib.Path(change_info[1])
                    if file_path != path:
                        if change_type == 'M' or change_type == '+':
                            tar_file.add(file_path, arcname=file_path.relative_to(path), recursive=False)
                        elif change_type == '-' or change_type == 'R':
                            file_path = file_path.parent.joinpath('.wh.' + file_path.name)
                            tar_file.add(wh_temp_file.name, arcname=file_path.relative_to(path), recursive=False)
                        if change_type == 'R':
                            file_path = pathlib.Path(change_info[2])
                            tar_file.add(file_path, arcname=file_path.relative_to(path), recursive=False)
        log.debug('Finish saving changeset (%s)' % str(changeset_file_path))
