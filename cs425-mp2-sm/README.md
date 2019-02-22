# cs425-mp1-sm

Repo for CS425 MP2

# Usage

### Run Server

```bash
$ python3 server.py

```

Then it will continuously print membership list for the node with default status
`LEAVED`. To join the group, use the following interface:

You are able to input directly to the terminal as command including

- `join`: join the node to group
- `leave`: leave the group
- `ml`: print current node's membership list
- `id`: print current node's id

_Note: To make things clear, the program automatically print membership list for
every 1 second by default. This may distract your input content but it still 
works even though your part of input is split by the print message._

The membership list includes

- host name
- id
- staus
- time stamp

For example

```
=== MembershipList on fa18-cs425-g33-01.cs.illinois.edu ===
fa18-cs425-g33-01.cs.illinois.edu: 57590 [RUNNING] [22:45:26]
fa18-cs425-g33-02.cs.illinois.edu: 17368 [RUNNING] [22:45:25]
fa18-cs425-g33-03.cs.illinois.edu: 51778 [RUNNING] [22:45:25]
fa18-cs425-g33-04.cs.illinois.edu: 7868 [RUNNING] [22:45:24]
fa18-cs425-g33-05.cs.illinois.edu: 26877 [RUNNING] [22:45:25]
fa18-cs425-g33-06.cs.illinois.edu: 23882 [FAILED] [22:25:38]
fa18-cs425-g33-07.cs.illinois.edu: 54677 [FAILED] [22:25:28]
fa18-cs425-g33-08.cs.illinois.edu: 4766 [RUNNING] [22:45:25]
fa18-cs425-g33-09.cs.illinois.edu: 2682 [RUNNING] [22:45:25]
fa18-cs425-g33-10.cs.illinois.edu: 49762 [RUNNING] [22:45:25]

```

# Developers
- [Wenhan Shi](mailto:wenhans2@illinois.edu)
- [Linling Miao](mailto:lmiao@illinois.edu)


