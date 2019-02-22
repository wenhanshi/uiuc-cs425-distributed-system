# CS425-MP4-SM: Stream Processing System

## Structure

We build a simple version of Storm system with the code from MP2 and MP3.
From MP2, we rebuild a master-follower version failure detector to enable
fault tolerance in the process of stream. From MP3, we embed each instance
of Supervisor (follower node) with a instance of SDFS, to read/write partial
results from SDFS. Both SDFS and Streaming system can get benefit from
failure detector. That is, they are both fault tolerant.

We use Nimbus (same name to Storm), a daemon in master node, to submit, schedule,
and detect failure. The system is able to run more than 1 Nimbus instances
at the same time to realize hot-standby.

We use Supervisor, a daemon in follower nodes, to run the streaming job.
Each follower has a config (or topology) with current job. They are no need
to get whole views of the picture. Just do the partial job and send result
to the child node.

## Usage

### Write Your App with `.yaml`

We use `.yaml` file in system to config application (i.e. topology). For example

```yaml
1:
  out: false
  op: 'lambda x: int(x) > 3'
  type: filter
  child: 2

2:
  out: true
  op: 'lambda x: pow(int(x), 2)'
  type: transform
  child: -1

```

Such application has two nodes: 1 and 2. The 1's child is 2. So the stream
firstly goes to node 1 and __filter all data that is larger than 3__. Then
the data goes to node 2 and __calculate its square__. Finally, node 2 is
an __out__ node, the result of stream will show in such __out__ node.

_Note: The node id like 1 and 2 are user-interfaced. The system will auto 
allocate real followers to all nodes in topology. You can find out the
relationship between node and machine with command `nm` on Nimbus._

### Run Nimbus

We hard-code that the master node with Nimbus daemon are only VM1 or VM2.
On these VMs, we can run Nimbus with

```bash
$ python3 nimbus.py

```

Nimbus has several available commands:

- `ml`: print current membership list
- `nm`: print current node map
- `topo`: print current topology config
- `submit [topo_file] [source]`: submit a job with specific topology and data source
- `run`: run the job with current config

### Run Supervisor

Similarly, Supervisor is able to run on VMs specified in `INIT_SUPERVISOR_IDS`.
The global variable is in `glob.py`.

```bash
$ python3 supervisor.py

```

Supervisor has several available commands:

- `jid`: print current job id
- `nm`: print current node map
- `save`: save current partial result to SDFS as e.g. result_23245.txt
- `lives`: print live nodes in SDFS
- `get [sdfs_file_name] [local_file_name]`: get file from SDFS
- `put [local_file_name] [sdfs_file_name]`: put file to SDFS
- `store`: list all file in SDFS

### Submit and Run Your Job

After the config of Nimbus and Supervisor, we can submit and run our job on Nimbus

```
--> submit demo.yaml test.txt
--> run
```

You are able to check topology with `topo` command to find out which node
is the out node, and get the result (both file and console) from that node.

## Developers
- [Wenhan Shi](mailto:wenhans2@illinois.edu)
- [Linling Miao](mailto:lmiao@illinois.edu)