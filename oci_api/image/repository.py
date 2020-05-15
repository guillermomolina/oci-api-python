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
from oci_spec.image.v1 import Index, ImageLayout
from oci_api.util import id_to_digest, digest_to_id
from oci_api import oci_config, OCIError
from oci_api.util.file import rm
from .image import Image
from .exceptions import ImageExistsException

log = logging.getLogger(__name__)

class Repository():
    def __init__(self, name):
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, name))
        self.name = name
        self.index = None
        self.images = {}
        repositories_path = pathlib.Path(oci_config['global']['path'], 'repositories')
        index_file_path = repositories_path.joinpath(self.name + '.json')
        if index_file_path.is_file():
            self.load()

    def load(self):
        repositories_path = pathlib.Path(oci_config['global']['path'], 'repositories')
        index_file_path = repositories_path.joinpath(self.name + '.json')
        log.debug('Start loading index file (%s)' % index_file_path)
        self.index = Index.from_file(index_file_path)
        for manifest_descriptor in self.index.get('Manifests'):
            manifest_annotations = manifest_descriptor.get('Annotations')
            manifest_tag = manifest_annotations.get('org.opencontainers.image.ref.name', None)
            manifest_descriptor_digest = manifest_descriptor.get('Digest')
            manifest_id = manifest_descriptor_digest.encoded()
            image = Image(self.name, manifest_tag)
            image.load(manifest_id)
            if image is not None:
                self.images[image.tag] = image
        log.debug('Finish loading index file (%s)' % index_file_path)

    def save(self):
        if self.index is None:
            raise OCIError('Can not save repository (%s), it is not initialized' % self.name)
        repositories_path = pathlib.Path(oci_config['global']['path'], 'repositories')
        if not repositories_path.is_dir():
            repositories_path.mkdir(parents=True)
        index_file_path = repositories_path.joinpath(self.name + '.json')
        self.index.save(index_file_path)

    def remove(self):
        if len(self.images) != 0:
            raise OCIError('There are (%d) images in the repository (%s), can not remove' 
                % (len(self.images), self.name))
        self.images = None
        self.index = None
        repositories_path = pathlib.Path(oci_config['global']['path'], 'repositories')
        index_file_path = repositories_path.joinpath(self.name + '.json')
        rm(index_file_path)

    def get_image(self, tag):
        try:
            return self.images[tag]
        except:
            raise OCIError('Image (%s) does not exist in this repository' % tag)

    def import_image(self, tag, rootfs_tar_path, image_config):
        image = self.images.get(tag, None)
        if image is not None:
            raise ImageExistsException('Image tag (%s) already exist' % tag)
        image = Image(self.name, tag)
        manifest_descriptor = image.create(rootfs_tar_path, image_config)
        manifest_descriptor.add('Annotations', {'org.opencontainers.image.ref.name': tag})
        self.images[tag] = image
        if self.index is None:
            self.index = Index(
                manifests=[manifest_descriptor]
            )
        else: 
            manifests = self.index.get('Manifests')
            manifests.append(manifest_descriptor)
        self.save()
        return image        

    def remove_image(self, tag):
        image = self.get_image(tag)
        manifest_digest = id_to_digest(image.id)
        image.remove()
        del self.images[tag]
        if self.index is None:
            raise OCIError('Can not remove image from repository (%s)' % self.name)
        manifests = self.index.get('Manifests')
        for index, manifest_descriptor in enumerate(manifests):
            if manifest_descriptor.get('Digest') == manifest_digest:
                manifests.pop(index)
                break
        if len(manifests) == 0:
            self.remove()
            return
        self.save()

    def save_image(self, tag, layout_path):
        oci_layout = ImageLayout(version='1.0.0')
        oci_layout_file_path = layout_path.joinpath('oci-layout')
        log.debug('Creating oci layout file (%s)' % oci_layout_file_path)
        oci_layout.save(oci_layout_file_path)
        image = self.get_image(tag)
        manifest_digest = id_to_digest(image.id)
        for manifest_descriptor in self.index.get('Manifests'):
            if manifest_descriptor.get('Digest') == manifest_digest:
                index = Index(manifests=[manifest_descriptor])
                index_file_path = layout_path.joinpath('index.json')
                log.debug('Creating index file (%s)' % index_file_path)
                index.save(index_file_path)
                image.export(layout_path)
                return

    def load_image(self, tag, layout_path):
        if tag in self.images:
            raise ImageExistsException('Image tag (%s) already exist' % tag)

        oci_file_path = layout_path.joinpath('oci-layout')
        if not oci_file_path.is_file():
            raise OCIError('There is no oci-layout file in (%s)' % str(layout_path))
        oci_layout = ImageLayout.from_file(oci_file_path)

        index_file_path = layout_path.joinpath('index.json')
        if not index_file_path.is_file():
            raise OCIError('There is no index.json file in (%s)' % str(layout_path))
        index = Index.from_file(index_file_path)
        manifest_descriptors = index.get('Manifests')
        if len(manifest_descriptors) == 0:
            raise OCIError('Index (%s) has no manifest' % str(index_file_path))
        if len(manifest_descriptors) > 1:
            raise OCIError('Index (%s) has (%d) manifests, only one supported' % 
            (str(index_file_path), len(manifest_descriptors)))
        manifest_descriptor = manifest_descriptors[0]
        manifest_digest = manifest_descriptor.get('Digest')
        manifest_id = digest_to_id(manifest_digest)
        manifest_descriptor.add('Annotations', {'org.opencontainers.image.ref.name': tag})

        image = Image(self.name, tag)
        image.copy(manifest_id, layout_path)
        self.images[tag] = image

        if self.index is None:
            self.index = index
        else: 
            manifests = self.index.get('Manifests')
            manifests.append(manifest_descriptor)
        self.save()
        return image        
