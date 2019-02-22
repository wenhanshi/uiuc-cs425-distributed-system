import socket
import json
from helper import *
from glob import *
import threading
from pprint import pprint
import sdfs


class Supervisor:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.nid = None
        self.jid = 99999
        self.node_map = None
        self.node_info = None
        self.next_host = None  # its child node id in current topology
        self.master_host = DEFAULT_MASTER_HOST  # current master host, to handle master failure
        self.t_data_receiver = threading.Thread(target=self.config_receiver)
        self.t_config_receiver = threading.Thread(target=self.data_receiver)
        self.t_ping_receiver = threading.Thread(target=self.ping_receiver)
        self.t_data_receiver.start()
        self.t_config_receiver.start()
        self.t_ping_receiver.start()
        self.t_monitor = threading.Thread(target=self.monitor)
        self.t_monitor.start()
        self.sdfs = sdfs.SDFSServer(self.host, DEFAULT_SDFS_PORT)
        self.join()

    def join(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            join_msg = {
                'type': 'join',
                'host': self.host,
                'mode': 'follower',
            }
            s.sendto(json.dumps(join_msg).encode('utf-8'), (self.master_host, DEFAULT_FD_PORT))

    def ping_receiver(self):
        """
        Receive ping message from current master.

        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, DEFAULT_FD_PORT))
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    msg_type = msg['type']
                    if msg_type == 'ping':
                        # return ack message to master
                        ack_msg = {
                            'type': 'ack',
                            'host': self.host,
                        }
                        self.master_host = msg['host']
                        s.sendto(json.dumps(ack_msg).encode('utf-8'), (self.master_host, DEFAULT_FD_PORT))

    def config_receiver(self):
        """
        Receiver config message from master including:
        - node: node-follower allocation
        - node_map: all topology graph

        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind((self.host, DEFAULT_SUPERVISOR_PORT))
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    msg_type = msg['type']
                    if msg_type == 'node':
                        self.nid = msg['nid']
                        self.node_info = msg['info']
                        self.jid = msg['jid']
                        self.master_host = msg['master_host']
                    elif msg_type == 'node_map':
                        self.node_map = {int(k): v for k, v in msg['info'].items()}
                        if self.nid in self.node_map and self.node_info['child'] != -1:
                            # once we get node_map, we can know real child host from node id
                            self.next_host = get_host_from_id(self.node_map[self.node_info['child']])

    def data_receiver(self):
        """
        Receive data (i.e. package in our Crane or tuple in Storm) from father node,
        apply the operation,
        and send the data to the child node.

        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as rs:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as ss:
                rs.bind((self.host, DEFAULT_DATA_PORT))
                while True:
                    data, server = rs.recvfrom(4096)
                    if data:
                        pkg = json.loads(data.decode('utf-8'))
                        if pkg['jid'] != self.jid:
                            continue
                        if self.node_info['type'] == 'filter':
                            self.filter(ss, pkg)
                        elif self.node_info['type'] == 'transform':
                            self.transform(ss, pkg)
                        elif self.node_info['type'] == 'join':
                            # relay all valid data to child node
                            ss.sendto(json.dumps(pkg).encode('utf-8'), (self.next_host, DEFAULT_DATA_PORT))

    def filter(self, ss, pkg):
        """
        Apply filter operation: if the data pass the filter, then it will be sent to child node.

        :param ss: socket connection
        :param pkg: data in Crane
        :return: None
        """
        if not eval(self.node_info['op'])(pkg['data']):
            return
        if self.node_info['out']:
            # output the data to file
            print(pkg['data'])
            with open('result_%05d.txt' % self.jid, 'a') as f:
                f.write(pkg['data'] + '\n')
        else:
            ss.sendto(json.dumps(pkg).encode('utf-8'), (self.next_host, DEFAULT_DATA_PORT))

    def transform(self, ss, pkg):
        """
        Apply transform operation and send to child node.

        :param ss: socket connection
        :param pkg: data in Crane
        :return: None
        """
        new_pkg = {
            'pid': pkg['pid'],
            'data': str(eval(self.node_info['op'])(pkg['data'])),
        }
        if self.node_info['out']:
            print(new_pkg['data'])
            with open('result_%05d.txt' % self.jid, 'a') as f:
                f.write(new_pkg['data'] + '\n')
        else:
            ss.sendto(json.dumps(new_pkg).encode('utf-8'), (self.next_host, DEFAULT_DATA_PORT))

    def save_result(self):
        """
        From command line, we can use 'save' command to save current partial result to SDFS.

        :return: None
        """
        base_path = os.path.expanduser('~')
        for obj in os.listdir(base_path):
            obj_path = os.path.join(base_path, obj)
            if obj == 'result_%05d.txt' % self.jid and os.path.isfile(obj_path):
                print('[INFO] Detect result file %s and save to SDFS.' % obj)
                self.sdfs.put_file(obj_path, obj)
                break

    def monitor(self):
        """
        It monitors the input command.

        :return: None
        """
        helper = '''
        === Supervisor Helper ===
        - jid (print current job id)
        - nm (print current node map)
        - save (save current partial result to SDFS as e.g. result_23245.txt)
        - lives (print live nodes in SDFS)
        - get [sdfs_file_name] [local_file_name] (get file from SDFS)
        - put [local_file_name] [sdfs_file_name] (put file to SDFS)
        - store (list all file in SDFS)
        '''
        print(helper)
        while True:
            arg = input('-->')
            args = arg.split(' ')
            if arg == '?' or arg == 'help':
                print(helper)
            elif arg == 'jid':
                print('Current job id: %d' % self.jid)
            elif arg == 'nm':
                pprint(self.node_map)
            elif arg == 'save':
                self.save_result()
            elif arg == 'lives':
                print(self.sdfs.lives)
            elif arg.startswith('get'):
                if len(args) != 3:
                    print('[ERROR FORMAT] get sdfs_file_name local_file_name')
                    continue
                self.sdfs.get_file(args[1], args[2])
            elif arg.startswith('put'):
                if len(args) != 3:
                    print('[ERROR FORMAT] put local_file_name sdfs_file_name')
                    continue
                self.sdfs.put_file(args[1], args[2])
            elif arg.startswith('store'):
                self.sdfs.show_store()
            else:
                print('[ERROR] Invalid input arg %s' % arg)


if __name__ == '__main__':
    supervisor = Supervisor(socket.gethostname(), DEFAULT_SUPERVISOR_PORT)
