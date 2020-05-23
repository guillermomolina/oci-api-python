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
import hashlib
import logging
import tempfile
from datetime import datetime 
from oci_spec.image.v1 import ImageConfig, Descriptor, \
    RootFS, History, Manifest, Image as Config, MediaTypeImageConfig, \
    MediaTypeImageManifest
from oci_api import oci_config, OCIError
from oci_api.util import digest_to_id, id_to_digest, architecture, operating_system, \
    normalize_image_name
from oci_api.util.file import rm, sha256sum, untar, cp
from oci_api.graph import Driver, LayerInUseException
from .exceptions import ImageInUseException

log = logging.getLogger(__name__)

def create_config():
    return Config(
        architecture=architecture(),
        os=operating_system(),
        rootfs=RootFS(rootfs_type='layers'),
        config=ImageConfig()
    )

def config_add_history(config, history_str, empty_layer=None):
    history = config.get('History') or []
    history.append(History(
        created=datetime.utcnow(), 
        created_by=history_str,
        empty_layer=empty_layer or False
    ))
    config.add('History', history)

def config_set_command(config, command, history_str=None):
    if history_str is not None:
        config_add_history(config, history_str, empty_layer=True)
    config.get('Config').add('Cmd', command)

def config_add_diff(config, diff_digest, history_str=None):
    root_fs = config.get('RootFS')
    diff_ids = root_fs.get('DiffIDs') or []
    diff_ids.append(diff_digest)
    root_fs.add('DiffIDs', diff_ids)
    if history_str is not None:
        config_add_history(config, history_str, empty_layer=False)

def save_config(config):
    log.debug('Start saving config')
    config_json = config.to_json(compact=True).encode()
    config_id = hashlib.sha256(config_json).hexdigest()
    configs_path = pathlib.Path(oci_config['global']['path'], 'configs')
    if not configs_path.is_dir():
        configs_path.mkdir(parents=True)
    config_file_path = configs_path.joinpath(config_id)
    config_file_path.write_bytes(config_json)
    config_descriptor = Descriptor(
        digest=id_to_digest(config_id),
        size=len(config_json),
        media_type=MediaTypeImageConfig
    )
    log.debug('Finish saving config')
    return config_descriptor

def create_manifest(config, layers):
    log.debug('Start creating manifest')
    config_descriptor = save_config(config)  
    layer_descriptors = [layer.descriptor for layer in layers]
    manifest = Manifest(
        config=config_descriptor,
        layers=layer_descriptors
    )
    log.debug('Finish creating manifest')
    return manifest

def save_manifest(manifest):
    log.debug('Start creating manifest')
    manifest_json = manifest.to_json(compact=True).encode()
    manifest_id = hashlib.sha256(manifest_json).hexdigest()
    manifests_path = pathlib.Path(oci_config['global']['path'], 'manifests')
    if not manifests_path.is_dir():
        manifests_path.mkdir(parents=True)
    manifest_file_path = manifests_path.joinpath(manifest_id)
    manifest_file_path.write_bytes(manifest_json)
    manifest_descriptor = Descriptor(
        digest=id_to_digest(manifest_id),
        size=len(manifest_json),
        media_type=MediaTypeImageManifest
    )
    log.debug('Finish saving manifest')
    return manifest_descriptor

class Image():
    @classmethod
    def create(cls, config, layers):
        image_config = config.get('Config')
        config.add('Created', datetime.utcnow())
        if len(layers) == 0:
            raise OCIError('Images must have at least one layer')
        manifest = create_manifest(config, layers)
        manifest_desciptor = save_manifest(manifest)
        image_id = manifest_desciptor.get('Digest').encoded()
        for layer in layers:
            Driver().add_image_reference(layer, image_id)
        return cls(image_id, [])

    def __init__(self, id, tags):
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, id))
        self.id = id
        self.tags = tags
        self.manifest = None
        self.config = None
        self.layers = None
        self.load()

    @property
    def name(self):
        if self.tags is not None and len(self.tags) > 0:
            return self.tags[0]
        return self.small_id

    @property
    def small_id(self):
        if self.id is not None:
            return self.id[:12]

    @property
    def digest(self):
        if self.id is not None:
            return id_to_digest(self.id)

    def top_layer(self):
        if self.layers is not None and len(self.layers) > 0:
            return self.layers[-1]
        return None

    def size(self):
        if self.layers is not None and len(self.layers) != 0:
            return sum([layer.size() for layer in self.layers])
        return None

    def virtual_size(self):
        if self.layers is not None and len(self.layers) != 0:
            return sum([layer.virtual_size() for layer in self.layers])

    def load(self, path=None):
        if self.id is None:
            raise OCIError('Can not load image without id')
        log.debug('Start loading image (%s) manifest' % self.id)
        manifests_path = path or pathlib.Path(oci_config['global']['path'], 'manifests')
        manifest_file_path = manifests_path.joinpath(self.id)
        self.manifest = Manifest.from_file(manifest_file_path)
        log.debug('Finish loading image (%s) manifest' % self.id)
        if path is not None:
            self.copy_manifest(manifest_file_path)
        self.load_config(path)

    def load_config(self, path=None):
        log.debug('Start loading image (%s) config' % self.id)
        if self.manifest is None:
            raise OCIError('Image (%s) has no manifest' % self.id)
        config_descriptor = self.manifest.get('Config')
        config_id = config_descriptor.get('Digest').encoded()
        log.debug('Loading image (%s) config (%s)' % (self.id, config_id))
        configs_path = path or pathlib.Path(oci_config['global']['path'], 'configs')
        config_file_path = configs_path.joinpath(config_id)
        self.config = Config.from_file(config_file_path)
        log.debug('Finish loading image (%s) config' % self.id)
        if path is not None:
            self.copy_config(config_file_path)
        self.load_layers(path)

    def load_layers(self, path=None):
        log.debug('Start loading image (%s) layers' % self.id)
        if self.config is None:
            raise OCIError('Image (%s) has no config' % self.id)
        if self.manifest is None:
            raise OCIError('Image (%s) has no manifest' % self.id)
        root_fs = self.config.get('RootFS')
        self.layers = []
        for layer_descriptor in self.manifest.get('Layers'):
            layer_id = layer_descriptor.get('Digest').encoded()
            log.debug('Loading image (%s) layer (%s)' % (self.id, layer_id))
            layer = Driver().get_layer(layer_id)
            self.layers.append(layer)
        log.debug('Finish loading image (%s) layers' % self.id)

    def destroy(self):
        if self.id is None:
            raise OCIError('Can not destroy image (%s)' % self.id)
        if len(Driver().get_child_filesystems(self.top_layer())) != 0:
            raise ImageInUseException()
        self.destroy_manifest()

    def destroy_manifest(self):
        log.debug('Start removing image (%s) manifest' % self.id)
        if self.manifest is None:
            raise OCIError('Image (%s) has no manifest' % self.id)
        self.remove_layers()
        self.destroy_config()
        manifests_path = pathlib.Path(oci_config['global']['path'], 'manifests')
        manifest_digest = self.digest
        manifest_file_path = manifests_path.joinpath(self.id)
        rm(manifest_file_path)
        self.history = None
        self.manifest = None
        log.info('Deleted: %s' % manifest_digest)
        log.debug('Finish removing image (%s) manifest' % self.id)

    def remove_layers(self):
        log.debug('Start removing image (%s) layers' % self.id)
        if self.layers is None:
            raise OCIError('Image (%s) has no layers' % self.id)
        for layer in reversed(self.layers):
            try:
                Driver().remove_image_reference(layer, self.id)
                Driver().remove_layer(layer)
            except LayerInUseException as e:
                log.debug(e.args[0])
        self.layers = None
        log.debug('Finish removing image (%s) layers' % self.id)

    def destroy_config(self):
        log.debug('Start removing image (%s) config' % self.id)
        if self.config is None:
            raise OCIError('Image (%s) has no config' % self.id)
        config_descriptor = self.manifest.get('Config')
        config_digest = config_descriptor.get('Digest')
        config_id = digest_to_id(config_digest)
        configs_path = pathlib.Path(oci_config['global']['path'], 'configs')
        config_file_path = configs_path.joinpath(config_id)
        rm(config_file_path)
        self.config = None
        log.info('Deleted: %s' % config_digest)
        log.debug('Finish removing image (%s) config' % self.id)

    def add_tag(self, tag):
        if tag in self.tags:
            raise OCIError('Image (%s) already has tag (%s)' % (self.id, tag))
        self.tags.append(tag)

    def remove_tag(self, tag):
        if tag not in self.tags:
            raise OCIError('Image (%s) does not have tag (%s)' % (self.id, tag))
        self.tags.remove(tag)