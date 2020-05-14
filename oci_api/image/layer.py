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
import shutil
import pathlib
from oci_spec.image.v1 import (
    Descriptor,
    MediaTypeImageLayerNonDistributable,
    MediaTypeImageLayerNonDistributableGzip,
)
from oci_api import oci_config, OCIError
from oci_api.util import id_to_digest
from oci_api.util.file import rm, compress, sha256sum
from oci_api.graph import Graph
from .exceptions import LayerInUseException

log = logging.getLogger(__name__)

class Layer:
    def __init__(self, id=None, diff_id=None, parent=None):
        log.debug('Creating instance of %s(%s)' % (type(self).__name__, id or ''))
        self.id = id
        self.diff_id = diff_id
        self.parent = parent
        self.filesystem = None

    @property
    def small_id(self):
        if self.id is not None:
            return self.id[:12]

    @property
    def digest(self):
        return id_to_digest(self.id)

    @property
    def diff_digest(self):
        return id_to_digest(self.diff_id)

    @property
    def size(self):
        return self.filesystem.size

    def child_filesystems(self):
        return Graph.driver().get_child_filesystems(self.filesystem)

    def create_child_filesystem(self, filesystem_id=None):
        return Graph.driver().create_filesystem(filesystem_id, self.filesystem)
        
    def load(self, blobs_path=None):
        if self.id is None:
            raise OCIError('Can not load layer without id')
        log.debug('Start loading layer (%s)' % self.id)
        if blobs_path is not None:
            layer_file_source_path = blobs_path.joinpath(self.id)
            log.debug('Start importing layer (%s)' % layer_file_source_path)
            layers_path = pathlib.Path(oci_config['global']['path'], 'layers')
            if not layers_path.is_dir():
                layers_path.mkdir()
            log.debug('Start copying layer file (%s)' % layer_file_source_path)
            shutil.copy(layer_file_source_path, layers_path)
            log.debug('Finish copying layer file (%s)' % layer_file_source_path)
            origin = None
            if self.parent is not None:
                origin = self.parent.filesystem
            filesystem = Graph.driver().create_filesystem(self.id, origin)
            layers_path = pathlib.Path(oci_config['global']['path'], 'layers')
            layer_file_path = layers_path.joinpath(self.id)
            filesystem.add_tar_file(layer_file_path)
            filesystem.commit()
            log.debug('Finish importing layer (%s)' % layer_file_source_path)
        self.filesystem = Graph.driver().get_filesystem(self.id)
        log.debug('Finish loading layer (%s)' % self.id)

    def remove(self):
        log.debug('Start removing layer (%s)' % self.id)
        if self.id is None:
            raise OCIError('Can not remove layer without id')
        if len(self.child_filesystems()) != 0:
            raise LayerInUseException('Layer (%s) is being used, can not remove')
        self.parent = None
        if self.filesystem is not None:
            log.debug('Start removing layer (%s) filesystem' % self.id)
            Graph.driver().remove_filesystem(self.filesystem)
            self.filesystem = None
            log.debug('Finish removing layer (%s) filesystem' % self.id)
        layers_path = pathlib.Path(oci_config['global']['path'], 'layers')
        layer_file_path = layers_path.joinpath(self.id)
        rm(layer_file_path)
        self.id = None   
        self.diff_id = None 
        log.debug('Finish removing layer (%s)' % self.id)

    def create(self, tar_file_path, compressed=True):
        log.debug('Start creating layer with file (%s)' % str(tar_file_path))
        layer_source_file_path = tar_file_path
        log.debug('Start getting hash of file (%s)' % str(layer_source_file_path))
        self.diff_id = sha256sum(layer_source_file_path)
        log.debug('Finish getting hash of file (%s)' % str(layer_source_file_path))
        if self.diff_id is None:
            raise OCIError('Could not get hash of file %s' % str(layer_source_file_path))
        self.id = self.diff_id
        media_type=MediaTypeImageLayerNonDistributable
        if compressed:
            log.debug('Start compressing file (%s)' % str(layer_source_file_path))
            layer_source_file_path = compress(layer_source_file_path, keep_original=True, method='gz', parallel=True)
            if layer_source_file_path is None:
                raise OCIError('Could not compress layer file (%s)' 
                    % str(tar_file_path))
            log.debug('Finish compressing file (%s)' % str(layer_source_file_path))
            media_type=MediaTypeImageLayerNonDistributableGzip
            log.debug('Start getting hash of file (%s)' % str(layer_source_file_path))
            self.id = sha256sum(layer_source_file_path)
            log.debug('Start getting hash of file (%s)' % str(layer_source_file_path))
            if self.id is None:
                raise OCIError('Could not get hash of file %s' % str(layer_source_file_path))
        layer_source_path = layer_source_file_path.parent
        layer_source_file_id_path = layer_source_path.joinpath(self.id)
        layer_source_file_path.rename(layer_source_file_id_path)
        self.load(layer_source_path)
        layers_path = pathlib.Path(oci_config['global']['path'], 'layers')
        layer_file_path = layers_path.joinpath(self.id)
        layer_descriptor = Descriptor(
            digest=id_to_digest(self.id),
            size=layer_file_path.stat().st_size,
            media_type=media_type,
        )
        log.debug('Finish creating layer with file (%s)' % str(tar_file_path))
        return layer_descriptor
