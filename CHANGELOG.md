# Change log for OCI API

## 2020-05-25: Version 0.5.0

- Moved commited filesystem path to diffs dir
- Moved mounted filesystem path to container dir
- Move Filesystem create() method to class
- Filesystem id is no longer diff id or container id, it is a random 16 caps letters and 
    number hash. Now diff_id is a Layer property and container_id is a property. 
- Layer size() is no longer calculated at runtime, it is stored as property at commit time
- Move Container create() method to class


## 2020-05-23: Version 0.4.0

- Add build process in Image
- Removed Distribution and Repository import image, "oci image import" uses build system
- Changed Image Distribution to be singleton in Image 
- Refactor Graph backend into Filesystem

## 2020-05-15: Version 0.3.0

- Added Graph ZFSFilesystem changeset
- Added zfs diff interface
- Removed extra directory (repository_tag/) in image save/load
- Modify Graph ZFSFilesystem id for layers from Layer id to Layer diff_id
- Added configured ZFS compression type
- Modified Image Layer and Graph ZFSFilesystem size calculation

## 2020-05-14: Version 0.2.0

- Simplified Graph ZFSDriver filesystem backend
- Added Image Distribution save for "oci image save"
- Added Image Distribution load for "oci image load"
- Added Graph Driver
- Added Graph ZFSDriver
- Added Graph Filesystem
- Added Graph ZFSFilesystem 
- Removed ZFS media type


## 2020-05-08: Version 0.1.3

- Refactor project, moved out oci and util to project "oci-api-python"
- Refactor project, moved out cli, to project "oci-cli-python"
- Renamed project to oci-api-python


## 2020-05-07: Version 0.1.2

- Added log system
- Added version info to commands
- Added "oci image import"
- Removed "mkimage" in favor of "oci image import -r"
- Added more checks to recognize valid ZFS in Graph ZFSDriver


## 2020-05-06: Version 0.1.1

- Added "oci container run"
- Added "oci container start"


## 2020-05-06: Version 0.1.0

- Added the runtime engine
- Added "oci container create"
- Added "oci container list"
- Added "oci container remove"
- Added "oci container inspect"
- Changed base_id lenght to 12 in Graph ZFSDriver
- Changed runc checks on run and create 


## 2020-05-04: Version 0.0.6

- Restructure /var/lib/oci layout


## 2020-04-28: Version 0.0.5

- Added the graph engine
- Added the Graph ZFSDriver
- Added the ZFS media type
- Added xz, lz and parallel compressing methods
- Added "oci image rm"


## 2020-04-22: Version 0.0.4

- Changed default directory to /var/lib/oci
- Added "mkimage"


## 2020-04-17: Version 0.0.3

- Added "oci image inspect"
- Split cli and api
- Added checks to OCI structs with python-oci package


## 2020-04-16: Version 0.0.2

- Added oci CLI
- Added "oci image ls"


## 2020-04-13: Version 0.0.1

- Initial export

