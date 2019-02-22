# CS425-MP3-SM: SDFS

## Usage

### Run Server

Similar to failure detector, we only need to run a single script to start both failure
detecor and SDFS srever (in different ports).

```bash
$ python3 sdfs.py

```

Then it will continuously print membership list for the node with default status
`LEAVED`. To join the group, use the following interface:

You are able to input directly to the terminal as command including

- `join`: join the node to group
- `leave`: leave the group
- `ml`: print current node's membership list

### SDFS Commands

Except for commands from failure detector (`join`, `leave` and `ml`), the
following commands are used to control SDFS:

- `get [sdfs_file_name] [local_file_name]`: get a (latest version) file from SDFS
- `get-versions [sdfs_file_name] [num-versions] [local_file_name]`: get versions file from SDFS
- `put [local_file_name] [sdfs_file_name]`: put a local to SDFS
- `delete [sdfs_file_name]`: delete a file in SDFS
- `ls [sdfs_file_name]`: list all replicas for a file stored in SDFS
- `store`: list all files store in SDFS and their replicas
- `lives`: check all current available replicas

## Developers
- [Wenhan Shi](mailto:wenhans2@illinois.edu)
- [Linling Miao](mailto:lmiao@illinois.edu)
