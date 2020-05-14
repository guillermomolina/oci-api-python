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
import shutil
import hashlib
import logging
from datetime import datetime 
from oci_spec.image.v1 import ImageConfig, Descriptor, \
    RootFS, History, Manifest, Image as Config, MediaTypeImageConfig, \
    MediaTypeImageManifest
from oci_api import oci_config, OCIError
from oci_api.util import digest_to_id, id_to_digest, architecture, operating_system
from oci_api.util.file import rm, sha256sum
from .layer import Layer
from .exceptions import ImageInUseException, LayerInUseException

log = logging.getLogger(__name__)

class Image():
    def __init__(self, repository, tag):
        log.debug('Creating instance of %s(%s, %s)' % (type(self).__name__, repository, tag))
        self.repository = repository
        self.tag = tag
        self.id = None
        self.manifest = None
        self.layers = None
        self.history = None
        self.config = None

    @property
    def small_id(self):
        if self.id is not None:
            return self.id[:12]

    @property
    def name(self):
        return '%s:%s' % (self.repository, self.tag)

    @property
    def digest(self):
        return id_to_digest(self.id)

    def top_layer(self):
        if self.layers is not None:
            return self.layers[-1]
        return None

    def size(self):
        if self.layers is not None and len(self.layers) != 0:
            return sum([layer.size for layer in self.layers])
        return None

    def virtual_size(self):
        log.warning('Method virtual_size() not implemented defaulting to size()')
        return self.size()

    def copy(self, manifest_id, repository_layout_path):
        blobs_path = repository_layout_path.joinpath('blobs', 'sha256')
        if not blobs_path.is_dir():
            raise OCIError('There is no blobs/sha56 directory in (%s)' % str(repository_layout_path))
        self.load(manifest_id, blobs_path)

    def load(self, manifest_id, blobs_path=None):
        self.id = manifest_id
        self.load_manifest(blobs_path)

    def load_manifest(self, blobs_path=None):
        log.debug('Start loading image (%s) manifest' % self.name)
        if self.id is None:
            raise OCIError('Image (%s) has no manifest id' % self.name)
        log.debug('Loading image (%s) manifest (%s)' % (self.name, self.id))
        manifests_path = blobs_path or pathlib.Path(oci_config['global']['path'], 'manifests')
        manifest_file_path = manifests_path.joinpath(self.id)
        self.manifest = Manifest.from_file(manifest_file_path)
        log.debug('Finish loading image (%s) manifest' % self.name)
        if blobs_path is not None:
            self.copy_manifest(manifest_file_path)
        self.load_config(blobs_path)

    def copy_manifest(self, manifest_file_source_path):
        log.debug('Start copying image (%s) manifest (%s)' % 
            (self.name, manifest_file_source_path))
        manifests_path = pathlib.Path(oci_config['global']['path'], 'manifests')
        if not manifests_path.is_dir():
            manifests_path.mkdir()
        shutil.copy(manifest_file_source_path, manifests_path)
        log.debug('Finish copying image (%s) manifest (%s)' % 
            (self.name, manifest_file_source_path))

    def load_config(self, blobs_path=None):
        log.debug('Start loading image (%s) config' % self.name)
        if self.manifest is None:
            raise OCIError('Image (%s) has no manifest' % self.name)
        config_descriptor = self.manifest.get('Config')
        config_descriptor_digest = config_descriptor.get('Digest')
        config_id = config_descriptor_digest.encoded()
        log.debug('Loading image (%s) config (%s)' % (self.name, config_id))
        configs_path = blobs_path or pathlib.Path(oci_config['global']['path'], 'configs')
        config_file_path = configs_path.joinpath(config_id)
        self.config = Config.from_file(config_file_path)
        log.debug('Finish loading image (%s) config' % self.name)
        if blobs_path is not None:
            self.copy_config(config_file_path)
        self.load_layers(blobs_path)

    def copy_config(self, config_file_source_path):
        log.debug('Start copying image (%s) config (%s)' % 
            (self.name, config_file_source_path))
        configs_path = pathlib.Path(oci_config['global']['path'], 'configs')
        if not configs_path.is_dir():
            configs_path.mkdir()
        shutil.copy(config_file_source_path, configs_path)
        log.debug('Finish copying image (%s) config (%s)' % 
            (self.name, config_file_source_path))

    def load_layers(self, blobs_path=None):
        log.debug('Start loading image (%s) layers' % self.name)
        if self.config is None:
            raise OCIError('Image (%s) has no config' % self.name)
        if self.manifest is None:
            raise OCIError('Image (%s) has no manifest' % self.name)
        root_fs = self.config.get('RootFS')
        layer_descriptors = self.manifest.get('Layers')
        diff_ids = root_fs.get('DiffIDs')
        if len(layer_descriptors) != len(diff_ids):
            raise OCIError('Layer descriptors count (%d) and diff ids count (%d) differ' %
                (len(layer_descriptors), len(diff_ids)))
        self.layers = []
        parent = None
        for layer_descriptor, diff_digest in zip(layer_descriptors, diff_ids):
            layer_digest = layer_descriptor.get('Digest')
            layer_id = digest_to_id(layer_digest)
            layer_diff_id = digest_to_id(diff_digest)
            log.debug('Loading image (%s) layer (%s)' % (self.name, layer_id))
            layer = Layer(layer_id, layer_diff_id, parent)
            layer.load(blobs_path)
            parent = layer
            self.layers.append(layer)
        log.debug('Finish loading image (%s) layers' % self.name)
    
    def save_manifest(self):
        log.debug('Start saving image (%s) manifest' % self.name)
        if self.manifest is None:
            raise OCIError('Image (%s) has no manifest' % self.name)
        manifest_json = self.manifest.to_json(compact=True).encode()
        manifest_id = hashlib.sha256(manifest_json).hexdigest()
        manifests_path = pathlib.Path(oci_config['global']['path'], 'manifests')
        if not manifests_path.is_dir():
            manifests_path.mkdir(parents=True)
        manifest_file_path = manifests_path.joinpath(manifest_id)
        manifest_file_path.write_bytes(manifest_json)
        self.id = manifest_id
        manifest_descriptor = Descriptor(
            digest=id_to_digest(manifest_id),
            size=len(manifest_json),
            media_type=MediaTypeImageManifest
        )
        log.debug('Finish saving image (%s) manifest' % self.name)
        return manifest_descriptor
    
    def save_config(self):
        log.debug('Start saving image (%s) config' % self.name)
        if self.config is None:
            raise OCIError('Image (%s) has no config', self.name)
        config_json = self.config.to_json(compact=True).encode()
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
        log.debug('Finish saving image (%s) config' % self.name)
        return config_descriptor

    def create(self, rootfs_tar_path, image_config):
        if self.manifest is not None:
            raise OCIError('Image (%s) already exists' % self.name)
        return self.create_manifest(rootfs_tar_path, image_config)

    def create_manifest(self, rootfs_tar_path, image_config):
        layer_descriptor = self.create_layer(rootfs_tar_path)
        config_descriptor = self.create_config(image_config)
        self.manifest = Manifest(
            config=config_descriptor,
            layers=[layer_descriptor]
        )
        return self.save_manifest()

    def create_layer(self, rootfs_tar_path, parent=None):
        layer = Layer(parent=parent)    
        layer_descriptor = layer.create(rootfs_tar_path)
        self.add_new_history('/bin/sh -c #(nop) ADD file:%s in / ' % layer.diff_id)
        if self.layers is None:
            self.layers = [layer]
        else: 
            self.layers.append(layer)
        return layer_descriptor

    def add_new_history(self, command, empty_layer=None):
        history = History(
            created=datetime.utcnow(), 
            created_by=command,
            empty_layer=empty_layer
        )
        if self.history is None:
            self.history = [history]
        else:
            self.history.append(history)
        return history

    def create_root_fs(self):
        if self.layers is None:
            raise ('Image (%s) has no layers', self.name)
        diff_ids = [layer.diff_digest for layer in self.layers]
        root_fs = RootFS(
            rootfs_type='layers', 
            diff_ids=diff_ids
        )
        return root_fs

    def create_config(self, image_config):
        command = image_config.get('Cmd')
        if command is not None:
            self.add_new_history('/bin/sh -c #(nop)  CMD ["%s"]' % ' '.join(command), 
                empty_layer=True)
        root_fs = self.create_root_fs()
        self.config = Config(
            created=datetime.utcnow(),
            architecture=architecture(),
            os=operating_system(),
            config=image_config,
            rootfs=root_fs,
            history=self.history
        )
        return self.save_config()

    def remove(self):
        if self.id is None:
            raise OCIError('Can not remove image (%s)' % self.name)
        # TODO: Test if image is in use
        if len(self.top_layer().child_filesystems()) != 0:
            raise ImageInUseException()
        self.remove_manifest()
        self.id = None

    def remove_manifest(self):
        if self.manifest is None:
            raise OCIError('Image (%s) has no manifest' % self.name)
        if self.id is None:
            raise OCIError('Image (%s) has no manifest id' % self.name)
        self.remove_layers()
        self.remove_config()
        manifests_path = pathlib.Path(oci_config['global']['path'], 'manifests')
        manifest_file_path = manifests_path.joinpath(self.id)
        rm(manifest_file_path)
        self.history = None
        self.manifest = None

    def remove_layers(self):
        if self.layers is None:
            raise OCIError('Image (%s) has no layers' % self.name)
        for layer in reversed(self.layers):
            try:
                layer.remove()
            except LayerInUseException:
                # Do not delete if it has children
                pass
        self.layers = None

    def remove_config(self):
        if self.config is None:
            raise OCIError('Image (%s) has no config' % self.name)
        config_descriptor = self.manifest.get('Config')
        config_id = digest_to_id(config_descriptor.get('Digest'))
        configs_path = pathlib.Path(oci_config['global']['path'], 'configs')
        config_file_path = configs_path.joinpath(config_id)
        rm(config_file_path)
        self.config = None

    def export(self, repository_layout_path):
        blobs_sha256_path = repository_layout_path.joinpath('blobs', 'sha256')
        blobs_sha256_path.mkdir(parents=True)
        manifests_path = pathlib.Path(oci_config['global']['path'], 'manifests')

        manifest_file_path = manifests_path.joinpath(self.id)
        manifest_blobs_sha256_path = blobs_sha256_path.joinpath(self.id)
        log.debug('Copying manifest file (%s)', manifest_blobs_sha256_path)
        shutil.copy(manifest_file_path, manifest_blobs_sha256_path)

        configs_path = pathlib.Path(oci_config['global']['path'], 'configs')
        config_descriptor = self.manifest.get('Config')
        config_id = digest_to_id(config_descriptor.get('Digest'))
        config_file_path = configs_path.joinpath(config_id)
        config_blobs_sha256_path = blobs_sha256_path.joinpath(config_id)
        log.debug('Copying config file (%s)', config_blobs_sha256_path)
        shutil.copy(config_file_path, config_blobs_sha256_path)

        layers_path = pathlib.Path(oci_config['global']['path'], 'layers')
        for layer in self.layers:
            if layer.id is None:
                raise OCIError('Can not export layer witouth id')
            layer_file_path = layers_path.joinpath(layer.id)
            layer_blobs_sha256_path = blobs_sha256_path.joinpath(layer.id)
            log.debug('Copying layer file (%s)', layer_blobs_sha256_path)
            shutil.copy(layer_file_path, layer_blobs_sha256_path)
