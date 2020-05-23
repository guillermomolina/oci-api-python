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


import subprocess
import secrets
import time
import logging
import shutil
from oci_api import OCIError

log = logging.getLogger(__name__)

def sha256sum(file_path):
    log.debug('Start getting hash of file (%s)' % file_path)
    cmd = ['/usr/bin/sha256sum', str(file_path)]
    log.debug('Running command: "' + ' '.join(cmd) + '"')
    sha256sum_run = subprocess.run(cmd, capture_output=True)
    sha256sum_result = None
    if sha256sum_run.returncode == 0:
        sha256sum_stdout = str(sha256sum_run.stdout.decode('utf-8'))
        records = sha256sum_stdout.split(' ')  
        sha256sum_result = records[0]
    log.debug('Finish getting hash of file (%s)' % file_path)
    return sha256sum_result

def tar(dir_path, tar_file_path=None, compress=False):
    if tar_file_path is None:
        tar_file_path_str = '-'
    else:
        tar_file_path_str = str(tar_file_path)
    log.debug('Start extracting from tar file (%s)' % tar_file_path_str)
    args = '-c'
    if compress:
        args += 'z'
    cmd = ['/usr/gnu/bin/tar', args, '-f', tar_file_path_str]
    cmd += [ str(f.relative_to(dir_path)) for f in dir_path.glob('*') ]
    log.debug('Running command: "cd ' + str(dir_path) + ';' + ' '.join(cmd) + '"')
    result = subprocess.call(cmd, cwd=dir_path)
    log.debug('Finish extracting from tar file (%s)' % tar_file_path_str)
    return result

def untar(dir_path, tar_file_path=None, tar_file=None):
    if tar_file_path is not None:
        tar_file_path_str = str(tar_file_path)
        stdin = None
    elif tar_file is not None:
        tar_file_path_str = '-'
        stdin = tar_file
    else:
        raise OCIError('untar error either tar_file_path or tar_file is needed')
    log.debug('Start inserting into tar file (%s)' % tar_file_path_str)
    cmd = ['/usr/gnu/bin/tar', '-x', '-f', tar_file_path_str]
    log.debug('Running command: "cd ' + str(dir_path) + ';' + ' '.join(cmd) + '"')
    result = subprocess.call(cmd, cwd=dir_path, stdin=stdin)
    log.debug('Finish inserting into tar file (%s)' % tar_file_path_str)
    return result

def uncompress(compressed_file_path, uncompressed_file_path=None, method='gz', 
        keep_original=False, force=True):
    log.debug('Start uncompressing file (%s)' % compressed_file_path)
    commands  = {
        'xz': ['/usr/bin/xzcat'],
        'gz': ['/usr/bin/gzcat'],
        'bz2': ['/usr/bin/bzcat'],
        'lz': ['/usr/bin/lzcat']
    }

    cmd = commands.get(method, None)
    if cmd is None:
        raise OCIError('method (%s) not supported' % method)
    cmd.append(str(compressed_file_path))

    if uncompressed_file_path is None:
        if compressed_file_path.suffix == 'method':
            uncompressed_file_path = compressed_file_path.parent.joinpath(compressed_file_path.stem)
        else:
            raise NotImplementedError()
    if uncompressed_file_path.is_file():
        if force:
            rm(uncompressed_file_path)
        else:
            raise OCIError('Target file (%s) allready exist, can not uncompress' % str(uncompressed_file_path))
    result = None
    with open(uncompressed_file_path, 'wb') as uncompressed_file:
        log.debug('Start running command: "' + ' '.join(cmd) + ' > %s"' % uncompressed_file_path)
        rc =  subprocess.call(cmd,  stdout=uncompressed_file)
        log.debug('Finish running command: "' + ' '.join(cmd) + ' > %s"' % uncompressed_file_path)
        if rc == 0:
            if not keep_original:
                rm(compressed_file_path)
            result = uncompressed_file_path
    log.debug('Finish uncompressing file (%s)' % compressed_file_path)
    return result

def compress(uncompressed_file_path, compressed_file_path=None, method='gz', 
        keep_original=False, force=True, parallel=True):
    log.debug('Start compressing file (%s)' % uncompressed_file_path)
    commands  = {
        'xz': ['/usr/bin/xz'],
        'gz': ['/usr/bin/gzip'],
        'bz2': ['/usr/bin/bzip2'],
        'lz': ['/usr/bin/lzma']
    }

    parallel_commands = {
        'gz': ['/usr/bin/pigz'],
        'xz': ['/usr/bin/xz', '-T', '0'],
    }
    if parallel:
        cmd = parallel_commands.get(method, None)
    else:
        cmd = commands.get(method, None)
    if cmd is None:
        raise OCIError('method (%s) not supported' % method)
    cmd.append('--stdout')
        
    cmd.append(str(uncompressed_file_path))
    if compressed_file_path is None:
        compressed_file_path = uncompressed_file_path.with_suffix(uncompressed_file_path.suffix +
            '.' + method)

    if compressed_file_path.is_file():
        if force:
            rm(compressed_file_path)
        else:
            raise OCIError('Target file (%s) allready exist, can not compress' % str(compressed_file_path))
    result = None
    with open(compressed_file_path, 'wb') as compressed_file:
        log.debug('Start running command: "' + ' '.join(cmd) + ' > %s"' % compressed_file_path)
        rc =  subprocess.call(cmd,  stdout=compressed_file)
        log.debug('Finish running command: "' + ' '.join(cmd) + ' > %s"' % compressed_file_path)
        if rc == 0:
            if not keep_original:
                rm(uncompressed_file_path)
            result = compressed_file_path
    log.debug('Finish compressing file (%s)' % uncompressed_file_path)
    return result

def du(dir_name):
    log.debug('Start calculating disk usage at (%s)' % str(dir_name))
    cmd = ['/usr/gnu/bin/du', '-bs', str(dir_name)]
    log.debug('Running command: "' + ' '.join(cmd) + '"')
    output = subprocess.check_output(cmd)
    value = output.decode('utf-8').split()[0]
    log.debug('Finish calculating disk usage at (%s)' % str(dir_name))
    return int(value)
        
def rm(file_name, retries=5, sleep=1, recursive=False):
    for i in range(retries):
        if not file_name.exists():
            return 0
        try:
            if file_name.is_dir():
                if recursive:
                    log.debug('Running: "rm -r' + str(file_name) + '"')
                    shutil.rmtree(file_name)
                else:    
                    log.debug('Running: "rmdir ' + str(file_name) + '"')
                    file_name.rmdir()
            else:
                log.debug('Running: "rm ' + str(file_name) + '"')
                file_name.unlink()
        except:
            log.exception('Could not delete path (%s) at attempt (%i)' % (
                str(file_name), i))
            time.sleep(sleep)
    return -1
      
def cp(src_file_path, dst_file_path):
    log.debug('Start copying (%s) to (%s)' % (src_file_path, dst_file_path))
    shutil.copy(src_file_path, dst_file_path)
    log.debug('Finish copying (%s) to (%s)' % (src_file_path, dst_file_path))
      
def mv(src_file_path, dst_file_path):
    log.debug('Start moving (%s) to (%s)' % (src_file_path, dst_file_path))
    shutil.move(src_file_path, dst_file_path)
    log.debug('Finish moving (%s) to (%s)' % (src_file_path, dst_file_path))
