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
# See the License for the configific language governing permissions and
# limitations under the License.

import json
import pathlib
import tempfile
import logging
from datetime import datetime, timezone
from oci_spec.runtime.v1 import Spec, Platform, Process, User, Root, Solaris, SolarisAnet, State
from oci_api import oci_config, OCIError
from oci_api.util import generate_random_sha256, digest_to_id, id_to_digest, operating_system, architecture
from oci_api.util.runc import runc_create, runc_delete, runc_exec, \
    runc_start
from oci_api.util.file import rm
from oci_api.graph import Driver

log = logging.getLogger(__name__)

def check_free_runc_id(runc_id):
    # TODO: check if there is no other runc container with id == self.runc_id
    # return True if we are ok to use it, False otherwise
    return True

class Container():
    @classmethod
    def create(cls, image, name, command=None, workdir=None):
        log.debug('Start creating container named (%s) from image (%s)' % (name, image.id))
        create_time = datetime.utcnow()
        os = image.config.get('OS')
        if os != operating_system():
            raise OCIError('Image (%s) operating system (%s) is not supported' % (image.id, os))
        arch = image.config.get('Architecture')
        if arch != architecture():
            raise OCIError('Image (%s) operating system (%s) is not supported' % (image.id, os))
        while True:
            container_id = generate_random_sha256()
            runc_id = container_id[:12]
            if check_free_runc_id(runc_id):
                break
        image_config_config = image.config.get('Config')
        args = command or image_config_config.get('Cmd') or ['/bin/sh']
        env = image_config_config.get('Env') or []
        cwd = workdir or image_config_config.get('WorkingDir') or '/'
        layer = image.top_layer()
        container_path = pathlib.Path(oci_config['global']['path'], 'containers', container_id)
        if not container_path.is_dir():
            container_path.mkdir(parents=True)
        rootfs_path = container_path.joinpath('rootfs')
        root_path = rootfs_path
        solaris = None
        if os == 'SunOS':
            solaris = Solaris(anet=[SolarisAnet()])
            root_path = rootfs_path.joinpath('root')
        filesystem = Driver().create_filesystem(layer)
        Driver().mount_filesystem(filesystem, container_id, root_path)
        config = Spec(
            platform=Platform(os=os, arch=arch),
            hostname=runc_id,
            process=Process(terminal=True, user=User(uid=0, gid=0), args=args, env=env, cwd=cwd),
            root=Root(path=str(rootfs_path), readonly=False),
            solaris=solaris
        )
        config_file_path = container_path.joinpath('config.json')
        config.save(config_file_path)
        runc_create(runc_id, container_path)
        log.debug('Finish creating container named (%s) from image (%s)' % (name, image.id))
        return cls(container_id, name, create_time)

    def __init__(self, id, name, create_time):
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, id))
        self.id = id
        self.name = name 
        self.create_time = create_time
        self.config = None
        self.load()

    @property
    def small_id(self):
        return self.id[:12]

    @property
    def runc_id(self):
        return self.small_id

    @property
    def state_change_time(self):
        zones_path = pathlib.Path('/var/run/zones/state')
        state_file_path = zones_path.joinpath(self.runc_id + '.state')
        if state_file_path.is_file():
            return datetime.fromtimestamp(state_file_path.lstat().st_mtime, 
                tz=timezone.utc)
        return None

    def status(self):
        container_state = self.state()
        if container_state is not None:
            return container_state.get('Status')
        return None

    def state(self):
        zones_path = pathlib.Path('/var/run/zones/state')
        state_file_path = zones_path.joinpath(self.runc_id + '.state')
        if state_file_path.is_file():
            return State.from_file(state_file_path)
        container_path = pathlib.Path(oci_config['global']['path'], 'containers', self.id)
        return State(
            id=self.runc_id,
            status='exited',
            bundlepath=str(container_path)
        )
    
    def to_json(self):
        return {
            'id': self.id,
            'name': self.name,
            'create_time': self.create_time.strftime('%Y-%m-%dT%H:%M:%S.%f000Z')
        }

    def load(self):
        container_path = pathlib.Path(oci_config['global']['path'], 'containers', self.id)
        config_file_path = container_path.joinpath('config.json')
        if not config_file_path.is_file():
            raise OCIError('Config file (%s) does not exist' % config_file_path)
        self.config = Spec.from_file(config_file_path)

    def destroy(self, remove_filesystem=True):
        container_id = self.id
        log.debug('Start destroying container (%s)' % container_id)
        container_status = self.status()
        if container_status != 'exited':
            self.delete_container()
        Driver().unmount_filesystem(self.id, remove=remove_filesystem)
        container_path = pathlib.Path(oci_config['global']['path'], 'containers', self.id)
        rm(container_path.joinpath('config.json'))
        rootfs_path = pathlib.Path(self.config.get('Root').get('Path'))
        if self.config.get('Platform').get('OS') == 'SunOS':
            rm(rootfs_path.joinpath('root'))
        rm(rootfs_path)
        rm(container_path)
        self.config = None
        self.id = None
        self.name = None
        self.create_time = None
        log.debug('Finish destroying container (%s)' % container_id)

    def delete_container(self):
        container_status = self.status
        force = container_status == 'running'
        runc_delete(self.runc_id, force)

    def exec(self, command, args=None):
        raise NotImplementedError()
        runc_exec(self.runc_id, command, args)

    def start(self):
        container_state = self.state()
        if container_state == 'created' or container_state == 'stopped':
            raise OCIError('Can not start container (%s) in state (%s)' % 
                (self.id, container_state))
        runc_start(self.runc_id)