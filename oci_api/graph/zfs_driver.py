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

import pathlib
import logging
import json
from oci_api import oci_config, OCIError
from oci_api.util import generate_random_sha256
from oci_api.util.zfs import zfs_create, zfs_is_filesystem
from .driver import Driver
from .zfs_filesystem import ZFSFilesystem
from .exceptions import FilesystemInUseException, FilesystemUnknownException

log = logging.getLogger(__name__)

class ZFSDriver(Driver):
    def __init__(self):
        super().__init__()
        log.debug('Creating instance of %s()' % type(self).__name__)
        self.filesystems = None
        self.driver_path = pathlib.Path(oci_config['global']['path'], 'zfs')
        if not self.driver_path.is_dir():
            self.driver_path.mkdir(parents=True)
        self.root_zfs = oci_config['graph']['zfs']['filesystem']
        compression = oci_config['graph']['zfs']['compression']
        if not zfs_is_filesystem(self.root_zfs):
            log.info('Creating root zfs (%s)' % self.root_zfs)
            self.root_zfs = zfs_create(self.root_zfs, mountpoint=self.driver_path, 
                compression=compression)
            if self.root_zfs is None:
                raise OCIError('Could not create root zfs (%s)' % root_zfs)   
        graph_path = pathlib.Path(oci_config['global']['path'])
        graph_file_path = graph_path.joinpath('graph.json')
        if graph_file_path.is_file():
            self.load()
        else:
            self.create()
    
    def create(self):
        graph_path = pathlib.Path(oci_config['global']['path'])
        graph_file_path = graph_path.joinpath('graph.json')
        log.debug('Start loading graph file (%s)' % graph_file_path)
        if graph_file_path.is_file():
            raise OCIError('Graph file (%s) already exist' % graph_file_path)
        if self.filesystems is not None:
            raise OCIError('Driver filesystems is already initialized' % graph_file_path)        
        self.filesystems = {}
        self.save()

    def load(self):
        graph_path = pathlib.Path(oci_config['global']['path'])
        graph_file_path = graph_path.joinpath('graph.json')
        log.debug('Start loading graph file (%s)' % graph_file_path)
        if not graph_file_path.is_file():
            raise OCIError('Graph file (%s) does not exist' % graph_file_path)
        if self.filesystems is not None:
            raise OCIError('Driver filesystems is already initialized' % graph_file_path)        
        self.filesystems = {}
        with graph_file_path.open() as graph_file:
            graph = json.load(graph_file)
            for filesystem_json in graph['filesystems']:
                filesystem_id = filesystem_json['id']
                log.debug('Found filesystem (%s)' % filesystem_id)
                origin_id = filesystem_json.get('origin_id', None)
                origin = self.get_filesystem(origin_id)
                filesystem = ZFSFilesystem(filesystem_id, origin)
                self.filesystems[filesystem_id] = filesystem
        log.debug('Finish loading graph file (%s)' % graph_file_path)

    def save(self):
        graph_path = pathlib.Path(oci_config['global']['path'])
        graph_file_path = graph_path.joinpath('graph.json')
        log.debug('Start saving graph file (%s)' % graph_file_path)
        if not graph_path.is_dir():
            graph_path.mkdir(parents=True)
        graph_json = {}
        if self.filesystems is None:
            raise OCIError('')
        graph_json['filesystems'] = self.save_filesystems(self.filesystems.values())
        with graph_file_path.open('w') as graph_file:
            json.dump(graph_json, graph_file, separators=(',', ':'))
        log.debug('Finish saving graph file (%s)' % graph_file_path)

    def save_filesystems(self, filesystems):
        filesystems_json = []
        for filesystem in filesystems:
            filesystem_json = {'id': filesystem.id}
            if filesystem.origin is not None:
                filesystem_json['origin_id'] = filesystem.origin.id
            filesystems_json.append(filesystem_json)
        return filesystems_json

    def get_filesystem(self, filesystem_id):
        if filesystem_id is None:
            return None
        if filesystem_id not in self.filesystems:
            raise FilesystemUnknownException('There is no filesystem with id (%s)' % filesystem_id)
        return self.filesystems[filesystem_id]

    def get_child_filesystems(self, origin):
        return [ 
            filesystem 
                for filesystem in self.filesystems.values()
                    if filesystem.origin is not None and filesystem.origin == origin 
        ]

    def remove_filesystem(self, filesystem):
        filesystem_id = filesystem.id
        log.debug('Start removing filesystem (%s)' % filesystem_id)
        if filesystem_id not in self.filesystems:
            raise OCIError('There is no filesystem with id (%s)' % filesystem_id)
        if len(self.get_child_filesystems(filesystem)):
            raise FilesystemInUseException('Filesystem (%s) is in use, can not delete' 
                % filesystem_id)
        filesystem.destroy()
        del self.filesystems[filesystem_id]
        log.debug('Finish removing filesystem (%s)' % filesystem_id)
        self.save()
    
    def create_filesystem(self, filesystem_id=None, origin=None):
        filesystem_id = filesystem_id or generate_random_sha256()
        log.debug('Start creating filesystem (%s)' % filesystem_id)
        if origin is not None and origin.id not in self.filesystems:
            raise OCIError('There is no filesystem with id (%s)' % origin.id)
        filesystem = ZFSFilesystem(filesystem_id, origin)
        filesystem.create()
        self.filesystems[filesystem_id] = filesystem
        log.debug('Finish creating filesystem (%s)' % filesystem_id)
        self.save()
        return filesystem
