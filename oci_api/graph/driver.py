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
from oci_spec.image.v1 import Descriptor
from oci_api import oci_config, OCIError
from oci_api.util import Singleton, generate_random_sha256
from .filesystem import Filesystem
from .layer import Layer
from .exceptions import FilesystemInUseException, FilesystemUnknownException, \
    LayerInUseException, LayerUnknownException

log = logging.getLogger(__name__)

class Driver(metaclass=Singleton):
    def __init__(self):
        super().__init__()
        log.debug('Creating instance of %s()' % type(self).__name__)
        self.filesystems = None
        driver_path = pathlib.Path(oci_config['global']['path'])
        driver_file_path = driver_path.joinpath('driver.json')
        if driver_file_path.is_file():
            self.load()
        else:
            self.create()

    def create(self):
        driver_path = pathlib.Path(oci_config['global']['path'])
        driver_file_path = driver_path.joinpath('driver.json')
        log.debug('Start loading driver file (%s)' % driver_file_path)
        if driver_file_path.is_file():
            raise OCIError('Driver file (%s) already exist' % driver_file_path)
        if self.filesystems is not None:
            raise OCIError('Driver filesystems is already initialized' % driver_file_path)        
        self.filesystems = {}
        self.layers = {}
        self.save()

    def load(self):
        driver_path = pathlib.Path(oci_config['global']['path'])
        driver_file_path = driver_path.joinpath('driver.json')
        log.debug('Start loading driver file (%s)' % driver_file_path)
        if not driver_file_path.is_file():
            raise OCIError('Driver file (%s) does not exist' % driver_file_path)
        if self.filesystems is not None:
            raise OCIError('Driver filesystems is already initialized' % driver_file_path)        
        with driver_file_path.open() as driver_file:
            driver_json = json.load(driver_file)
            if oci_config['driver']['type'] != driver_json['type']:
                # Convertion between drivers
                raise NotImplementedError()
            self.filesystems = {}
            self.layers = {}
            for filesystem_json in driver_json.get('filesystems', []):
                self.load_filesystem(filesystem_json)
        log.debug('Finish loading driver file (%s)' % driver_file_path)

    def load_filesystem(self, filesystem_json, layer=None):
        filesystem_id = filesystem_json['id']
        log.debug('Loading filesystem (%s)' % filesystem_id)
        container_id = filesystem_json.get('container_id', None)
        filesystem = Filesystem(filesystem_id, layer, container_id)
        layer_json = filesystem_json.get('layer')
        if layer_json is not None:
            self.load_layer(layer_json, filesystem)
        self.filesystems[filesystem_id] = filesystem

    def load_layer(self, layer_json, filesystem):
        layer_descriptor = Descriptor.from_json(layer_json['descriptor'])
        log.debug('Loading layer (%s)' % layer_descriptor.get('Digest').encoded())
        diff_id = layer_json['diff_id']
        size = layer_json['size']
        images = layer_json.get('images', [])
        layer = Layer(layer_descriptor, diff_id, filesystem, size, images)
        self.layers[layer.id] = layer
        for filesystem_json in layer_json.get('filesystems', []):
            self.load_filesystem(filesystem_json, layer)

    def save(self):
        driver_path = pathlib.Path(oci_config['global']['path'])
        driver_file_path = driver_path.joinpath('driver.json')
        log.debug('Start saving driver file (%s)' % driver_file_path)
        if self.filesystems is None:
            raise OCIError('Driver filesystems is not initialized, can not save')
        filesystems_json = [
            self.filesystem_to_json(filesystem)
                for filesystem in self.filesystems.values()
                    if filesystem.layer is None
        ]
        if not driver_path.is_dir():
            driver_path.mkdir(filesystems=True)
        with driver_file_path.open('w') as driver_file:
            driver_json = {
                'type': oci_config['driver']['type'], 
                'filesystems': filesystems_json
            }
            json.dump(driver_json, driver_file, separators=(',', ':'))
        log.debug('Finish saving driver file (%s)' % driver_file_path)

    def filesystem_to_json(self, filesystem):
        filesystem_json = {'id': filesystem.id}
        if filesystem.container_id is not None:
            filesystem_json['container_id'] = filesystem.container_id
        layer = self.get_child_layer(filesystem)
        if layer is not None:
            filesystem_json['layer'] = self.layer_to_json(layer)
        return filesystem_json

    def layer_to_json(self, layer):
        layer_json = {
            'descriptor': layer.descriptor.to_dict(),
            'diff_id': layer.diff_id,
            'size': layer.size
        }
        if len(layer.images) > 0:
            layer_json['images'] = layer.images
        filesystems = self.get_child_filesystems(layer)
        if len(filesystems) > 0:
            layer_json['filesystems'] = [self.filesystem_to_json(filesystem) for filesystem in filesystems]
        return layer_json

    def get_filesystem(self, filesystem_id):
        if filesystem_id not in self.filesystems:
            raise FilesystemUnknownException('There is no filesystem with id (%s)' % filesystem_id)
        return self.filesystems[filesystem_id]

    def get_filesystem_by_container_id(self, container_id):
        for filesystem in self.filesystems.values():
            if filesystem.container_id == container_id:
                return filesystem
        raise FilesystemUnknownException('There is no filesystem mounted for container (%s)' % container_id)

    def get_child_filesystems(self, layer):
        return [ 
            filesystem 
                for filesystem in self.filesystems.values()
                    if filesystem.layer == layer 
        ]
    
    def create_filesystem(self, layer=None):
        log.debug('Start creating filesystem')
        if layer is not None and layer.id not in self.layers:
            raise LayerUnknownException('There is no layer with id (%s)' % layer.id)
        filesystem = Filesystem.create(layer)
        self.filesystems[filesystem.id] = filesystem
        self.save()
        log.debug('Finish creating filesystem')
        return filesystem

    def mount_filesystem(self, filesystem, container_id, path):
        log.debug('Start mounting filesystem (%s) for container (%s)' % (filesystem.id, container_id))
        if filesystem.id not in self.filesystems:
            raise FilesystemUnknownException('Filesystem (%s) is not in driver, can not mount'
                % filesystem.id)
        filesystem.mount(container_id, path)
        self.save()
        log.debug('Finish mounting filesystem (%s) for container (%s)' % (filesystem.id, container_id))

    def unmount_filesystem(self, container_id, remove=False):
        filesystem = self.get_filesystem_by_container_id(container_id)
        log.debug('Start unmounting filesystem (%s) for container (%s)' % (filesystem.id, container_id))
        filesystem.unmount(container_id)
        if remove:
            self.remove_filesystem(filesystem)
        self.save()
        log.debug('Finish unmounting filesystem (%s) for container (%s)' % (filesystem.id, container_id))

    def remove_filesystem(self, filesystem):
        filesystem_id = filesystem.id
        log.debug('Start removing filesystem (%s)' % filesystem_id)
        if filesystem_id not in self.filesystems:
            raise FilesystemUnknownException('Filesystem (%s) is not in driver, can not remove'
                % filesystem_id)
        if filesystem.is_mounted() or self.get_child_layer(filesystem) is not None:
            raise FilesystemInUseException('Filesystem (%s) is in use, can not remove' 
                % filesystem_id)
        filesystem.destroy()
        del self.filesystems[filesystem_id]
        self.save()
        log.debug('Finish removing filesystem (%s)' % filesystem_id)

    def get_layer(self, layer_id):
        '''if layer_id is None:
            return None'''
        if layer_id not in self.layers:
            raise LayerUnknownException('There is no layer with id (%s)' % layer_id)
        return self.layers[layer_id]

    def get_layer_by_diff_id(self, diff_id):
        for layer in self.layers.values():
            if layer.diff_id == diff_id:
                return layer
        raise LayerUnknownException('There is no layer with diff id (%s)' % diff_id)

    def get_child_layer(self, filesystem):
        for layer in self.layers.values():
            if layer.filesystem == filesystem:
                return layer
        # If no layer is pointing to filesystem, it is a temp filesystem
    
    def create_layer(self, filesystem):
        original_filesystem_id = filesystem.id
        log.debug('Start creating layer from filesystem (%s)' % original_filesystem_id)
        if filesystem.id not in self.filesystems:
            raise FilesystemUnknownException('Unknown filesystem (%s)' % filesystem.id)
        layer = Layer.create(filesystem)
        self.layers[layer.id] = layer
        self.filesystems[layer.filesystem.id] = layer.filesystem
        self.save()
        log.debug('Finish creating layer from (%s)' % original_filesystem_id)
        return layer

    def remove_layer(self, layer):
        layer_id = layer.id
        log.debug('Start removing layer (%s)' % layer_id)
        if layer_id not in self.layers:
            raise LayerUnknownException('Layer (%s) is not in driver, can not remove'
                % layer_id)
        if len(layer.images) > 0:
            images = ', '.join(layer.images)
            raise LayerInUseException('Layer (%s) is used by %s, can not remove' % (layer_id, images))
        if len(self.get_child_filesystems(layer)):
            raise LayerInUseException('Layer (%s) is in use, can not remove' 
                % layer_id)
        layer_filesystem = layer.filesystem
        layer.destroy()
        del self.layers[layer_id]
        self.remove_filesystem(layer_filesystem) 
        log.debug('Finish removing layer (%s)' % layer_id)

    def add_image_reference(self, layer, image_id):
        layer_id = layer.id
        log.debug('Start adding image reference (%s) to layer (%s)' % (image_id, layer_id))
        if layer_id not in self.layers:
            raise LayerUnknownException('Layer (%s) is not in driver, can not add image ref'
                % layer_id)
        layer.add_image_reference(image_id)
        self.save()
        log.debug('Finish adding image reference (%s) to layer (%s)' % (image_id, layer_id))

    def remove_image_reference(self, layer, image_id):
        layer_id = layer.id
        log.debug('Start removing layer (%s) reference to image (%s)' % (layer_id, image_id))
        if layer_id not in self.layers:
            raise LayerUnknownException('Layer (%s) is not in driver, can not add image ref'
                % layer_id)
        layer.remove_image_reference(image_id)
        self.save()
        log.debug('Finish removing layer (%s) reference to image (%s)' % (layer_id, image_id))
