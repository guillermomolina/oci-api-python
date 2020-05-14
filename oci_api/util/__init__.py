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

import platform

from .random import generate_random_sha256, generate_random_id, \
    generate_random_name

def digest_to_id(digest):
    if digest is not None:
        return digest.split(':')[1]

def id_to_digest(id):
    if id is not None:
        return 'sha256:' + id 


def architecture():
    architectures = {
        'sparc': 'sparc64', 
        'i386': 'amd64'
    }
    return architectures[platform.processor()]

def operating_system():
    return platform.system()