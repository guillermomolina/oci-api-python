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
from oci_spec.distribution.v1 import RepositoryList
from oci_api import oci_config, OCIError
from .repository import Repository
from .exceptions import ImageInUseException, ImageUnknownException

log = logging.getLogger(__name__)

def split_image_name(image_name):
    splitted_image_name = image_name.split(':')            
    if len(splitted_image_name) == 1:
        splitted_image_name.append('latest')
    elif len(splitted_image_name) != 2:
        raise OCIError('Invalid image name (%s)' % image_name)
    return tuple(splitted_image_name)

class Distribution():
    def __init__(self):
        log.debug('Creating instance of %s()' % type(self).__name__)
        self.repositories = None
        distribution_file_path = pathlib.Path(oci_config['global']['path'], 
            'distribution.json')
        if distribution_file_path.is_file():
            self.load()
        else:
            self.create()

    def load(self):
        distribution_path = pathlib.Path(oci_config['global']['path'])
        distribution_file_path = distribution_path.joinpath('distribution.json')
        log.debug('Start loading distribution file (%s)' % distribution_file_path)
        if not distribution_file_path.is_file():
            raise OCIError('Distribution file (%s) does not exist' % distribution_file_path)
        repository_list = RepositoryList.from_file(distribution_file_path)
        self.repositories = {}
        for repository_name in repository_list.get('Repositories'):
            log.debug('Found repository (%s)' % repository_name)
            repository = Repository(repository_name)
            self.repositories[repository_name] = repository
        log.debug('Finish loading distribution file (%s)' % distribution_file_path)

    def create(self):
        distribution_file_path = pathlib.Path(oci_config['global']['path'], 
            'distribution.json')
        log.debug('Start creating distribution file (%s)' % distribution_file_path)
        if distribution_file_path.is_file():
            raise OCIError('Distribution file (%s) already exists' % distribution_file_path)
        self.repositories = {}
        log.debug('Finish creating distribution file (%s)' % distribution_file_path)
        self.save()

    def save(self):
        distribution_path = pathlib.Path(oci_config['global']['path'])
        distribution_file_path = distribution_path.joinpath('distribution.json')
        log.debug('Start saving distribution file (%s)' % distribution_file_path)
        if not distribution_path.is_dir():
            distribution_path.mkdir(parents=True)
        repository_list_json = {
            'repositories': list(self.repositories.keys())
        }
        repository_list = RepositoryList.from_json(repository_list_json)
        repository_list.save(distribution_file_path)
        log.debug('Finish saving distribution file (%s)' % distribution_file_path)

    def get_repository(self, repository_name):
        try:
            return self.repositories[repository_name]
        except:
            raise ImageUnknownException('Repository (%s) does not exist' % repository_name)

    def get_image(self, image_ref):        
        # image_ref, can either be:
        # small id (6 bytes, 12 octets, 96 bits), the first 12 octets from id
        # id (16 bytes, 32 octets, 256 bits), the sha256 hash
        # name of the repository (tag implied as latest)
        # repository_name:tag
        repository_name, tag = split_image_name(image_ref)
        for repository in self.repositories.values():
            for image in repository.images.values():                
                if image.id == image_ref:
                    return image
                if image.small_id == image_ref:
                    return image
                if image.repository == repository_name and image.tag == tag:
                    return image
        raise ImageUnknownException('Image (%s) is unknown' % image_ref)

    def remove_image(self, image_name):
        log.debug('Start removing image (%s)' % image_name)
        image = self.get_image(image_name)
        repository_name = image.repository
        tag = image.tag
        repository = self.get_repository(repository_name)
        repository.remove_image(tag)
        if repository.index is None:
            del self.repositories[repository_name]
            self.save()
        log.debug('Finish removing image (%s)' % image_name)

    def save_image(self, image_name, layout_path):
        log.debug('Start exporting image (%s)' % image_name)
        repository_name, tag = split_image_name(image_name)
        repository = self.get_repository(repository_name)
        repository.save_image(tag, layout_path)
        log.debug('Finish exportig image (%s)' % image_name)

    def load_image(self, image_name, layout_path):
        log.debug('Start importing image (%s)' % image_name)
        repository_name, tag = split_image_name(image_name)
        repository = self.repositories.get(repository_name, Repository(repository_name))
        image = repository.load_image(tag, layout_path)
        if repository_name not in self.repositories:
            self.repositories[repository_name] = repository
            self.save()
        log.debug('Finish importing image (%s)' % image_name)
        return image

    def import_image(self, image_name, rootfs_tar_path, image_config):
        log.debug('Start creating image (%s)' % image_name)
        repository_name, tag = split_image_name(image_name)
        repository = self.repositories.get(repository_name, Repository(repository_name))
        image = repository.import_image(tag, rootfs_tar_path, image_config)
        if repository_name not in self.repositories:
            self.repositories[repository_name] = repository
            self.save()
        log.debug('Finish creating image (%s)' % image_name)
        return image
