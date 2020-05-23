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
from oci_api import oci_config, OCIError
from oci_api.util import Singleton, normalize_image_name, split_image_name
from oci_api.util.file import rm
from .image import Image
from .exceptions import ImageInUseException, ImageUnknownException, TagUnknownException

log = logging.getLogger(__name__)

class Distribution(metaclass=Singleton):
    def __init__(self):
        log.debug('Creating instance of %s()' % type(self).__name__)
        self.images = None
        distribution_path = pathlib.Path(oci_config['global']['path'])
        distribution_file_path = distribution_path.joinpath('distribution.json')
        if distribution_file_path.is_file():
            self.load()
        else:
            self.create()

    def load(self):
        if self.images is not None:
            raise OCIError('Distribution already loaded')
        distribution_path = pathlib.Path(oci_config['global']['path'])
        distribution_file_path = distribution_path.joinpath('distribution.json')
        log.debug('Start loading distribution file (%s)' % distribution_file_path)
        if not distribution_file_path.is_file():
            raise OCIError('Distribution file (%s) does not exist' % distribution_file_path)
        with distribution_file_path.open() as distribution_file:
            self.images = {}
            images_json = json.load(distribution_file)
            for image_json in images_json['images']:
                image_id = image_json['id']
                tags = image_json['tags'] or []
                from .image import Image
                image = Image(image_id, tags)
                self.images[image_id] = image
        log.debug('Finish loading distribution file (%s)' % distribution_file_path)

    def create(self):
        distribution_path = pathlib.Path(oci_config['global']['path'])
        distribution_file_path = distribution_path.joinpath('distribution.json')
        log.debug('Start creating distribution file (%s)' % distribution_file_path)
        if self.images is not None:
            raise OCIError('Distribution repositories is already initialized, can not create')
        if distribution_file_path.is_file():
            raise OCIError('Distribution file (%s) already exists' % distribution_file_path)
        self.images = {}
        self.save()
        log.debug('Finish creating distribution file (%s)' % distribution_file_path)

    def save(self):
        distribution_path = pathlib.Path(oci_config['global']['path'])
        distribution_file_path = distribution_path.joinpath('distribution.json')
        log.debug('Start saving distribution file (%s)' % distribution_file_path)
        if self.images is None:
            raise OCIError('Distribution is not initialized, can not save')
        if not distribution_path.is_dir():
            distribution_path.mkdir(parents=True)
        images_json = []
        for image in self.images.values():
            images_json.append({
                'id': image.id,
                'tags': image.tags
            })
        distribution_json = {
            'images': images_json
        }
        with distribution_file_path.open('w') as distribution_file:
            json.dump(distribution_json, distribution_file, separators=(',', ':'))
        log.debug('Finish saving distribution file (%s)' % distribution_file_path)

    def get_repositories(self, image):
        tags = image.tags
        repositories = []
        for repository_and_tag in tags:
            (repository, tag) = split_image_name(repository_and_tag)
            repositories.append(repository)
        return repositories

    def get_image_by_id(self, image_id):        
        for image in self.images.values():                
            if image.id == image_id:
                return image
            if image.small_id == image_id:
                return image
        raise ImageUnknownException('Image (%s) is unknown' % image_id)
        
    def get_image(self, image_ref):        
        # image_ref, can either be:
        # - small id (6 bytes, 12 octets, 96 bits), the first 12 octets from id
        # - id (16 bytes, 32 octets, 256 bits), the sha256 hash
        # - name (tag implied as latest)
        # - name:tag
        try:
            return self.get_image_by_id(image_ref)
        except ImageUnknownException:
            image_name = normalize_image_name(image_ref)
            for image in self.images.values():
                if image_name in image.tags:
                    return image
        raise ImageUnknownException('Image (%s) is unknown' % image_ref)

    def create_image(self, config, layers):
        log.debug('Start creating image')
        image = Image.create(config, layers)
        self.images[image.id] = image
        self.save()
        log.debug('Finish creating image (%s)' % image.id)
        return image

    def remove_image(self, image, force=False):
        image_id = image.id
        log.debug('Start removing image (%s)' % image_id)
        if self.images is None:
            raise ImageUnknownException('Distribution images is not initialized, can not remove image (%s)' % image_id)
        if image_id not in self.images:
            raise ImageUnknownException('Image is unknown, can not remove image (%s)' % image_id)
        image.destroy()
        del self.images[image_id]
        self.save()
        log.debug('Finish removing image (%s)' % image_id)

    def add_tag(self, image, tag):
        log.debug('Start adding tag (%s) to image (%s)' % (tag, image.id))
        normalized_tag = normalize_image_name(tag)
        try:
            other_image = self.get_image(normalized_tag)
            if other_image != image:
                self.remove_tag(other_image, normalized_tag)
                image.add_tag(normalized_tag)
                self.save()
        except ImageUnknownException:
            image.add_tag(normalized_tag)
            self.save()
        log.debug('Finish adding tag (%s) to image (%s)' % (tag, image.id))

    def remove_tag(self, image, tag):
        log.debug('Start removing tag (%s) from image (%s)' % (tag, image.id))
        normalized_tag = normalize_image_name(tag)
        image.remove_tag(normalized_tag)
        self.save()
        log.debug('Finish removing tag (%s) from image (%s)' % (tag, image.id))
