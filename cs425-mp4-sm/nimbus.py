import socket
import json
from helper import *
from glob import *
import random
import time
import datetime
import threading
from pprint import pprint
import yaml


class Topology:
    def __init__(self, nodes):
        self.root_id = min(nodes.keys())
        self.nodes = nodes


class Nimbus:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.topo = None  # topology nodes
        self.topo_file = None  # topo file (.yaml file)
        self.source = None  # data source file (e.g. .txt)
        self.node_map = {}  # from node id to host id
        self.pid = 0  # package id, package is tuple in Storm system
        self.jid = -1  # job id, every new job has a unique id
        self.ml = {}  # membership list, like failure detector
        self.timer = {}  # failure checker
        self.ml_lock = threading.Lock()
        self.master_host = DEFAULT_MASTER_HOST
        self.t_receiver = threading.Thread(target=self.receiver)
        self.t_receiver.start()
        self.t_ping_sender = threading.Thread(target=self.ping_sender)
        self.t_ping_sender.start()
        self.t_checker = threading.Thread(target=self.checker)
        self.t_checker.start()
        self.t_monitor = threading.Thread(target=self.monitor)
        self.t_monitor.start()
        self.join()

    def join(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            join_msg = {
                'type': 'join',
                'host': self.host,
                'mode': 'master',
            }
            s.sendto(json.dumps(join_msg).encode('utf-8'), (self.master_host, DEFAULT_FD_PORT))

    def receiver(self):
        """
        Receiver thread on Nimbus. It responsible for all incoming message:
        - 'ack/join/leave' from follower
        - 'ping' from another hot-standby master
        - 'sync' from current master (as hot-standby itself)

        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, DEFAULT_FD_PORT))
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    from_host = msg['host']
                    if msg['type'] == 'join':
                        self.ml_lock.acquire()
                        self.ml[from_host] = {
                            'mode': msg['mode'],
                            'status': Status.RUNNING,
                        }
                        self.ml_lock.release()
                        if from_host in self.timer:
                            del self.timer[from_host]
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as ss:
                            join_msg = {
                                'type': 'join',
                                'lives': list(INIT_SUPERVISOR_IDS & {get_id_from_host(k) for k in self.ml}),
                            }
                            for host in ALL_HOSTS:
                                ss.sendto(json.dumps(join_msg).encode('utf-8'), (host, DEFAULT_SDFS_PORT))
                    elif msg['type'] == 'ack':
                        if from_host in self.timer:
                            del self.timer[from_host]
                    elif msg['type'] == 'leave':
                        if from_host not in self.ml:
                            continue
                        self.ml[from_host]['status'] = Status.LEAVED
                        if from_host in self.timer:
                            del self.timer[from_host]
                    elif msg['type'] == 'sync' and self.master_host != self.host:
                        self.topo_file = msg['topo_file']
                        self.source = msg['source']
                        self.node_map = msg['node_map']
                        self.pid = msg['pid']
                        self.jid = msg['jid']
                        self.ml = msg['ml']
                    elif msg['type'] == 'ping':
                        ack_msg = {
                            'type': 'ack',
                            'host': self.host,
                        }
                        s.sendto(json.dumps(ack_msg).encode('utf-8'), (from_host, DEFAULT_FD_PORT))

    def ping_sender(self):
        """
        A ping sender. Multicast ping message to all followers.

        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                time.sleep(1)
                ping_msg = {
                    'type': 'ping',
                    'host': self.host,
                }
                for host in list(self.ml.keys()):
                    if (host == self.host) or (self.ml[host]['status'] == Status.LEAVED):
                        continue
                    if self.ml[host]['mode'] == 'master':
                        # send sync message to hot-standby
                        sync_msg = {
                            'type': 'sync',
                            'topo_file': self.topo_file,
                            'source': self.source,
                            'node_map': self.node_map,
                            'pid': self.pid,
                            'jid': self.jid,
                            'ml': self.ml,
                            'host': self.host,
                        }
                        s.sendto(json.dumps(sync_msg).encode('utf-8'), (host, DEFAULT_FD_PORT))
                        s.sendto(json.dumps(ping_msg).encode('utf-8'), (host, DEFAULT_FD_PORT))
                        if host not in self.timer:
                            self.timer[host] = datetime.datetime.now()

                    if self.ml[host]['mode'] == 'follower' and self.host == self.master_host:
                        # send ping message to all followers
                        s.sendto(json.dumps(ping_msg).encode('utf-8'), (host, DEFAULT_FD_PORT))
                        if host not in self.timer:
                            self.timer[host] = datetime.datetime.now()

    def checker(self):
        """
        A failure-detector-liked checker with timeout feature.

        :return: None
        """
        while True:
            for host in list(self.timer.keys()):
                now = datetime.datetime.now()
                time_delta = now - self.timer.get(host, datetime.datetime.now())
                if time_delta.days >= 0 and time_delta.seconds > 3.:
                    if self.ml[host]['status'] == Status.FAILED:
                        continue
                    self.ml[host]['status'] = Status.FAILED
                    cur_ids = set(self.node_map.values())
                    if self.ml[host]['mode'] == 'master':
                        # master fault tolerance: switch to hot-standby and resubmit current job
                        if host == self.master_host:
                            print('[INFO] Master failure detected. Change master to [%s].' % self.host)
                            self.master_host = self.host
                            if self.topo_file is not None:
                                print('[INFO] Resubmit the job.')
                                self.submit(self.topo_file, self.source)
                                self.run_job(self.jid)
                    else:
                        # follower fault tolerance: simply resubmit current the job or not (if not related)
                        print('[INFO] Follower [%s] failure detected.' % host)
                        if get_id_from_host(host) in cur_ids:
                            print('[INFO] Resubmit the job.')
                            self.submit(self.topo_file, self.source)
                            self.run_job(self.jid)
                        else:
                            print('[INFO] No need to resubmit the job.')
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                            failed_msg = {
                                'type': 'failed',
                                'host': host,
                            }
                            for live_host in self.ml:
                                s.sendto(json.dumps(failed_msg).encode('utf-8'), (live_host, DEFAULT_SDFS_PORT))

    def run_job(self, jid):
        """
        Create a new instance of run() process to submit multiple jobs without stopping the previous.

        :return:
        """
        t_run = threading.Thread(target=self.run, args=[jid])
        t_run.start()

    def monitor(self):
        """
        It monitors the input command.

        :return: None
        """
        helper = '''
        === Nimbus Helper ===
        - ml (print current membership list)
        - nm (print current node map)
        - topo (print current topology config)
        - submit [topo_file] [source] (submit a job with specific topology and data source)
        - run (run the job with current config)
        '''
        print(helper)
        while True:
            arg = input('-->')
            args = arg.split(' ')
            if arg == '?' or arg == 'help':
                print(helper)
            elif arg == 'ml':
                pprint(self.ml)
            elif arg == 'nm':
                pprint(self.node_map)
            elif arg == 'topo':
                if not self.topo:
                    print('[ERROR] No topology.')
                    continue
                pprint(self.topo.nodes)
            elif arg.startswith('submit'):
                if len(args) != 3:
                    print('[ERROR FORMAT] submit topo_file source')
                    continue
                self.submit(args[1], args[2])
            elif arg == 'run':
                self.run_job(self.jid)
            else:
                print('[ERROR] Invalid input arg %s' % arg)

    def get_topo_from_file(self, file_path):
        """
        Parse .yaml file to topology class.

        :param file_path: path of .yaml file
        :return:
        """
        nodes = yaml.load(open(file_path, 'r'))
        return Topology(nodes)

    def get_live_follower_ids(self):
        return {get_id_from_host(k)
                for k, v in self.ml.items()
                if v['status'] == Status.RUNNING and v['mode'] == 'follower'}

    def submit(self, topo_file, source):
        """
        Submit a job with topo_file and source. Job may be resubmitted if failure is detected.

        :param topo_file: .yaml file name
        :param source: data file (simply, we use .txt as input source, each line is a raw tuple)
        :return:
        """
        self.pid = 0
        self.topo = self.get_topo_from_file(topo_file)
        self.topo_file = topo_file
        if not self.topo:
            print('[ERROR] Invalid topology.')
            return
        self.source = source
        self.jid = random.randint(0, 65535)
        self.node_map = {}  # reschedule node map
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            cur_ids = self.get_live_follower_ids()
            for nid, node_info in self.topo.nodes.items():
                # for each node in topology, allocate a available follower to it
                remain_ids = cur_ids - set(self.node_map.values())
                if not remain_ids:
                    print('[ERROR] No remain empty nodes.')
                    return
                schedule_id = random.choice(list(remain_ids))
                node_msg = {
                    'type': 'node',
                    'nid': nid,
                    'jid': self.jid,
                    'info': node_info,
                    'master_host': self.host,
                }
                s.sendto(json.dumps(node_msg).encode('utf-8'),
                         (get_host_from_id(schedule_id), DEFAULT_SUPERVISOR_PORT))
                self.node_map[nid] = schedule_id
            # once all nodes are allocated, broadcast a complete node map info to all followers
            node_map_msg = {
                'type': 'node_map',
                'info': self.node_map,
            }
            for nid in INIT_SUPERVISOR_IDS:
                s.sendto(json.dumps(node_map_msg).encode('utf-8'),
                         (get_host_from_id(nid), DEFAULT_SUPERVISOR_PORT))

    def run(self, jid):
        """
        Run a job with current config (i.e. topology and data source).

        :return:
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            with open(self.source, 'r') as f:
                for line in f:
                    if jid != self.jid:
                        break
                    if INPUT_SLEEP_PERIOD != .0:
                        time.sleep(INPUT_SLEEP_PERIOD)
                    pkg = {
                        'pid': self.pid,
                        'jid': jid,
                        'data': line.rstrip('\n'),
                    }
                    try:
                        s.sendto(json.dumps(pkg).encode('utf-8'),
                                 (get_host_from_id(self.node_map[self.topo.root_id]), DEFAULT_DATA_PORT))
                    except Exception as e:
                        pass
                    self.pid += 1


if __name__ == '__main__':
    nimbus = Nimbus(socket.gethostname(), DEFAULT_DATA_PORT)
