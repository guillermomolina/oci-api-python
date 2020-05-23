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
import tempfile
from oci_spec.image.v1 import Descriptor, MediaTypeImageLayer, MediaTypeImageLayerGzip
from oci_api import oci_config, OCIError
from oci_api.util import id_to_digest
from oci_api.util.file import rm, cp, compress, sha256sum

log = logging.getLogger(__name__)

class Layer:
    @classmethod
    def create(cls, filesystem, compressed=True):
        filesystem_id = filesystem.id
        log.debug('Start creating layer from filesystem (%s)' % filesystem.id)
        with tempfile.TemporaryDirectory() as temp_dir_name:
            changeset_file_path = pathlib.Path(temp_dir_name, 'changeset.tar')
            filesystem.commit(changeset_file_path)
            layer_id = filesystem.id
            media_type=MediaTypeImageLayer
            if compressed:
                changeset_file_path = compress(changeset_file_path, keep_original=True)
                if changeset_file_path is None:
                    raise OCIError('Could not compress layer file (%s)' % str(changeset_file_path))
                media_type=MediaTypeImageLayerGzip
                layer_id = sha256sum(changeset_file_path)
                if layer_id is None:
                    raise OCIError('Could not get hash of file %s' % str(changeset_file_path))
            layers_path = pathlib.Path(oci_config['global']['path'], 'layers')
            layer_file_path = layers_path.joinpath(layer_id)
            if not layer_file_path.is_file():
                if not layers_path.is_dir():
                    layers_path.mkdir(parents=True)
                cp(changeset_file_path, layer_file_path)
        descriptor = Descriptor(
            digest=id_to_digest(layer_id),
            size=layer_file_path.stat().st_size,
            media_type=media_type,
        )
        layer = cls(descriptor, filesystem, [])
        log.debug('Finish creating layer from filesystem (%s)' % filesystem_id)
        return layer

    def __init__(self, descriptor, filesystem, images):
        self.descriptor = descriptor
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, self.id or ''))
        self.filesystem = filesystem
        self.images = images

    @property
    def id(self):
        return self.descriptor.get('Digest').encoded()

    @property
    def parent(self):
        self.filesystem.layer

    @property
    def diff_id(self):
        self.filesystem.id

    @property
    def small_id(self):
        return self.id[:12]

    @property
    def digest(self):
        return id_to_digest(self.id)

    @property
    def diff_digest(self):
        return self.filesystem.digest

    def size(self):
        return self.filesystem.size()

    def virtual_size(self):
        return self.filesystem.virtual_size()

    def destroy(self):
        log.debug('Start destroying layer (%s)' % self.id)
        layers_path = pathlib.Path(oci_config['global']['path'], 'layers')
        layer_file_path = layers_path.joinpath(self.id)
        rm(layer_file_path)
        layer_digest = self.digest
        layer_id = self.id
        self.descriptor = None   
        self.filesystem = None
        log.info('Deleted: %s' % layer_digest)
        log.debug('Finish destroying layer (%s)' % layer_id)

    def add_image_reference(self, image_id):
        if image_id in self.images:
            raise OCIError('Layer (%s) already has image (%s)' % (self.id, image_id))
        self.images.append(image_id)

    def remove_image_reference(self, image_id):
        if image_id not in self.images:
            raise OCIError('Layer (%s) does not have image (%s)' % (self.id, image_id))
        self.images.remove(image_id)        