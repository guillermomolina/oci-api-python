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
from oci_spec.image.v1 import Index as ImageIndex, ImageLayout
from oci_api.util import id_to_digest, digest_to_id
from oci_api import oci_config, OCIError
from oci_api.util.file import rm
from .image import Image
from .exceptions import ImageExistsException

log = logging.getLogger(__name__)

class Index():
    def __init__(self):
        log.debug('Creating instance of %s' % type(self).__name__)
        self.index = None
        self.images = {}
        images_path = pathlib.Path(oci_config['global']['path'], 'images')
        index_file_path = images_path.joinpath('index.json')
        if index_file_path.is_file():
            self.load()
        else:
            self.create()

    def load(self):
        log.debug('Start loading index')
        repositories_path = pathlib.Path(oci_config['global']['path'], 'images')
        index_file_path = repositories_path.joinpath('index.json')
        self.index = ImageIndex.from_file(index_file_path)
        for manifest_descriptor in self.index.get('Manifests'):
            image = Image(manifest_descriptor)
            image.load()
            if image is not None:
                self.images[image.id] = image
        log.debug('Start loading index')

    def save_index(self):
        log.debug('Start saving index')
        if self.index is None:
            raise OCIError('Can not save repository (%s), it is not initialized' % self.name)
        repositories_path = pathlib.Path(oci_config['global']['path'], 'images')
        if not repositories_path.is_dir():
            repositories_path.mkdir(parents=True)
        index_file_path = repositories_path.joinpath('index.json')
        self.index.save(index_file_path)
        log.debug('Finish saving index')

    def remove(self):
        log.debug('Start removing index')
        if len(self.images) != 0:
            raise OCIError('There are (%d) images in the repository (%s), can not remove' 
                % (len(self.images), self.name))
        self.images = None
        self.index = None
        repositories_path = pathlib.Path(oci_config['global']['path'], 'images')
        index_file_path = repositories_path.joinpath('index.json')
        rm(index_file_path)
        log.debug('Finish removing index')

    def get_image(self, tag):
        try:
            return self.images[tag]
        except:
            raise OCIError('Image (%s) does not exist in this repository' % tag)

    def create_image(self, tag, from_image=None):
        log.debug('Start creating image tagged (%s) to repository (%s)' % (tag,self.name))
        image = self.images.get(tag, None)
        if image is not None:
            raise ImageExistsException('Image tag (%s) already exist' % tag)
        image = Image(self.name, tag)
        image.create(from_image)
        self.images[tag] = image
        log.debug('Finish creating image tagged (%s) to repository (%s)' % (tag,self.name))
        return image        

    def remove_image(self, tag):
        log.debug('Start removing image tagged (%s) from repository (%s)' % (tag,self.name))
        image = self.get_image(tag)
        manifest_digest = id_to_digest(image.id)
        image.remove()
        del self.images[tag]
        if self.index is None:
            raise OCIError('Can not remove image from index')
        manifests = self.index.get('Manifests')
        for index, manifest_descriptor in enumerate(manifests):
            if manifest_descriptor.get('Digest') == manifest_digest:
                manifests.pop(index)
                break
        if len(manifests) == 0:
            self.remove()
            return
        log.debug('Finish removing image tagged (%s) from repository (%s)' % (tag,self.name))
        self.save()

    def save_image(self, tag, layout_path=None):
        log.debug('Start saving image tagged (%s) from repository (%s)' % (tag,self.name))
        if layout_path is not None:
            oci_layout = ImageLayout(version='1.0.0')
            oci_layout_file_path = layout_path.joinpath('oci-layout')
            log.debug('Creating oci layout file (%s)' % oci_layout_file_path)
            oci_layout.save(oci_layout_file_path)
        image = self.get_image(tag)
        if image.id is None:   
            manifest_descriptor = image.create_manifest()
            manifest_descriptor.add('Annotations', {'org.opencontainers.image.ref.name': tag})
            self.images[tag] = image
            if self.index is None:
                self.index = ImageIndex(manifests=[manifest_descriptor])
            else: 
                manifests = self.index.get('Manifests')
                manifests.append(manifest_descriptor)
            self.save()
        else:        
            manifest_digest = id_to_digest(image.id)
            for manifest_descriptor in self.index.get('Manifests'):
                if manifest_descriptor.get('Digest') == manifest_digest:
                    break
        if layout_path is not None:
            index = ImageIndex(manifests=[manifest_descriptor])
            index_file_path = layout_path.joinpath('index.json')
            log.debug('Creating index file (%s)' % index_file_path)
            index.save(index_file_path)
            image.export(layout_path)
        
        log.debug('Finish saving image tagged (%s) from repository (%s)' % (tag,self.name))

    def load_image(self, tag, layout_path):
        log.debug('Start loading image tagged (%s) from repository (%s)' % (tag,self.name))
        if tag in self.images:
            raise ImageExistsException('Image tag (%s) already exist' % tag)

        oci_file_path = layout_path.joinpath('oci-layout')
        if not oci_file_path.is_file():
            raise OCIError('There is no oci-layout file in (%s)' % str(layout_path))
        oci_layout = ImageLayout.from_file(oci_file_path)

        index_file_path = layout_path.joinpath('index.json')
        if not index_file_path.is_file():
            raise OCIError('There is no index.json file in (%s)' % str(layout_path))
        index = ImageIndex.from_file(index_file_path)
        manifest_descriptors = index.get('Manifests')
        if len(manifest_descriptors) == 0:
            raise OCIError('ImageIndex (%s) has no manifest' % str(index_file_path))
        if len(manifest_descriptors) > 1:
            raise OCIError('ImageIndex (%s) has (%d) manifests, only one supported' % 
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
        log.debug('Finish loading image tagged (%s) from repository (%s)' % (tag,self.name))
        self.save()
        return image        
