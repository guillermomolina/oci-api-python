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

import json
import pathlib
import logging
from dateutil import parser
from oci_api import oci_config, OCIError
from oci_api.util import Singleton, generate_random_name
from .container import Container
from .exceptions import ContainerUnknownException

log = logging.getLogger(__name__)

class Runtime(metaclass=Singleton):
    def __init__(self):
        log.debug('Creating instance of %s()' % type(self).__name__)
        self.containers = None
        runtime_path = pathlib.Path(oci_config['global']['path'])
        runtime_file_path = runtime_path.joinpath('runtime.json')
        if runtime_file_path.is_file():
            self.load()
        else:
            self.create()

    def load(self):
        runtime_path = pathlib.Path(oci_config['global']['path'])
        runtime_file_path = runtime_path.joinpath('runtime.json')
        with runtime_file_path.open() as runtime_file:
            runtime = json.load(runtime_file)
            self.containers = {}
            for container_json in runtime.get('containers', []):
                container_id = container_json['id']
                name = container_json['name']
                create_time = parser.isoparse(container_json['create_time'])
                container = Container(container_id, name, create_time)
                self.containers[container_id] = container

    def create(self):
        runtime_path = pathlib.Path(oci_config['global']['path'])
        runtime_file_path = runtime_path.joinpath('runtime.json')
        if runtime_file_path.is_file():
            raise OCIError('Runtime file (%s) already exists' % runtime_file_path)
        self.containers = {}
        self.save()

    def save(self):
        runtime_path = pathlib.Path(oci_config['global']['path'])
        if not runtime_path.is_dir():
            runtime_path.mkdir(parents=True)
        containers = [container.to_json() for container in self.containers.values()]
        runtime_json = {
            'containers': containers
        }
        runtime_file_path = runtime_path.joinpath('runtime.json')
        with runtime_file_path.open('w') as runtime_file:
            json.dump(runtime_json, runtime_file, separators=(',', ':'))

    def generate_container_name(self):
        container_names = [container.name for container in self.containers.values()]
        return generate_random_name(exclude_list=container_names)

    def create_container(self, image, name=None, **kwargs):
        if name is None:
            name = self.generate_container_name()
        container = Container.create(image, name, **kwargs)
        self.containers[container.id] = container
        self.save()
        return container

    def remove_container(self, container_ref, remove_filesystem=True):
        container = self.get_container(container_ref)
        container_id = container.id
        container.destroy(remove_filesystem)
        del self.containers[container_id]
        self.save()

    def get_container(self, container_ref):        
        # container_ref, can either be:
        # small id (6 bytes, 12 octets, 96 bits), the first 12 octets from id
        # id (16 bytes, 32 octets, 256 bits), the sha256 hash
        # name of the container
        for container in self.containers.values():
            if container.id == container_ref:
                return container
            if container.small_id == container_ref:
                return container
            if container.name == container_ref:
                return container
        raise ContainerUnknownException('Container (%s) is unknown' % container_ref)

    def get_containers_using_image(self, image_id):
        return [
            container 
                for container in self.containers.values() 
                    if container.image.id == image_id
        ]