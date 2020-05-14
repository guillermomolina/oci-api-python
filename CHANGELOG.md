# Change log for OCI API

## 2020-05-14: Version 0.2.0

- Simplified Graph ZFSDriver filesystem backend
- Added Image save for "oci image save"
- Added Image load for "oci image load"
- Added Graph Driver Filesystem
- Added Graph ZFSDriver ZFSFilesystem 
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

