import socket
import threading
import json
import subprocess
import shutil
from glob import *
from pprint import pprint
import random


class FileTable:
    def __init__(self):
        self.idm = {i + 1: set() for i in range(10)}  # id mapper: id -> all files in such id
        self.fm = {}  # file mapper: file name -> all file features (replicas and versions)

    def insert_file(self, sdfs_file_name, ids):
        """
        Given a sdfs file and replicas, insert the file to all these replicas and
        update fm and idm.
        :param sdfs_file_name: SDFS file name
        :param ids: e.g. {3, 4, 5, 6}
        :return: None
        """
        for id in ids:
            self.idm[id].add(sdfs_file_name)
        if sdfs_file_name not in self.fm:
            self.fm[sdfs_file_name] = {
                'version': 0,
                'replicas': set(),
            }
        self.fm[sdfs_file_name]['version'] += 1
        self.fm[sdfs_file_name]['replicas'] |= ids

    def delete_file(self, sdfs_file_name):
        """
        Given a sdfs file, delete the file info in fm and idm.
        :param sdfs_file_name: SDFS file name
        :return: None
        """
        for id in self.idm:
            self.idm[id].discard(sdfs_file_name)
        if sdfs_file_name in self.fm:
            del self.fm[sdfs_file_name]


class SDFSServer:
    def __init__(self, host, port):
        self.ft = FileTable()  # file table, including an id mapper and file mapper
        self.host = host
        self.port = port
        self.id = self.get_id_from_host(host)
        self.addr = (self.host, self.port)
        self.lives = {self.id}  # record all available sources, i.e. replicas
        if os.path.exists(SDFS_PATH):
            shutil.rmtree(SDFS_PATH)
        os.mkdir(SDFS_PATH)
        self.t_receiver = threading.Thread(target=self.receiver)
        self.t_receiver.start()

    def receiver(self):
        """
        Like failure detector, receiver is an UDP receiver to get all contact message including
        - UPDATE: update idm and fm, sync the status
        - DELETE: alert an deletion operation
        - FAILED_RELAY: receive from failure detector, to know that a replica is down
        - FAILED: multicasted by replicas, to let all other node know a replica is down
        - JOIN: multicasted by sdfs server whose failure detector is introducer (default is node with id 1)
        :return: None
        """
        fm = self.ft.fm
        idm = self.ft.idm
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)
            while True:
                data, server = s.recvfrom(4096)
                if data:
                    msg = json.loads(data.decode('utf-8'))
                    msg_type = msg['type']

                    if msg_type == 'update':
                        fn = msg['file_name']
                        replicas = set(msg['replicas'])
                        for replica in replicas:
                            idm[replica].add(fn)
                        if fn not in fm:
                            fm[fn] = {
                                'version': 0,
                                'replicas': set(),
                            }
                        fm[fn]['version'] = msg['version']
                        fm[fn]['replicas'] |= replicas

                    elif msg_type == 'delete':
                        fn = msg['file_name']
                        self.ft.delete_file(fn)
                        # search and delete sdfs replica from local storage
                        for file in os.listdir(SDFS_PATH):
                            file_path = os.path.join(SDFS_PATH, file)
                            if os.path.isfile(file_path) and file.startswith(fn):
                                print('[INFO] Match and delete file %s.' % file)
                                os.remove(file_path)

                    elif msg_type == 'failed':
                        fid = self.get_id_from_host(msg['host'])
                        if fid not in self.lives:
                            continue
                        self.lives.discard(fid)
                        for f in idm[fid]:
                            replicas = fm[f]['replicas']
                            replicas.discard(fid)
                            # check if itself needs to help re-replicate
                            try:
                                if self.id == max(replicas):
                                    # choice an available source
                                    rid = random.choice(list(self.lives - replicas))
                                    # help to re-replicate
                                    for file in os.listdir(SDFS_PATH):
                                        file_path = os.path.join(SDFS_PATH, file)
                                        if os.path.isfile(file_path) and file.startswith(f):
                                            print('[INFO] Re-replica file %s to %d' % (file, rid))
                                            prefix = 'wenhans2' + '@' + self.get_host_from_id(rid)
                                            p = subprocess.Popen(['scp',
                                                                  file_path,
                                                                  prefix + ':' + file_path])
                                            # os.waitpid(p.pid, 0)
                                    # update status and new replica message
                                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as ss:
                                        update_msg = {
                                            'type': 'update',
                                            'file_name': f,
                                            'replicas': list({rid} | replicas),
                                            'version': fm[f]['version'],
                                        }
                                        for host in ALL_HOSTS:
                                            ss.sendto(json.dumps(update_msg).encode('utf-8'), (host, self.port))
                            except Exception as e:
                                pass
                        # clean idm, in case other failed message comes
                        idm[fid] = set()

                    elif msg_type == 'join':
                        self.lives |= set(msg['lives'])

    def get_id_from_host(self, host):
        """
        e.g. fa18-cs425-g33-01.cs.illinois.edu -> 1
        :param host: host name
        :return: an integer id
        """
        return int(host.split('.')[0].split('-')[-1])

    def get_host_from_id(self, host_id):
        """
        e.g. 1 -> fa18-cs425-g33-01.cs.illinois.edu
        :param host_id: int id
        :return: host str
        """
        return 'fa18-cs425-g33-%02d.cs.illinois.edu' % host_id

    def put_file(self, local_file_name, sdfs_file_name):
        """
        Put a local file (unix-like path) to SDFS, with the name of sdfs_file_name.
        :param local_file_name: e.g. /foo/bar/a.txt
        :param sdfs_file_name: e.g. foo.sdfs (flatten system)
        :return:
        """
        if not os.path.exists(local_file_name):
            print('[ERROR] No such local file: %s' % local_file_name)
            return
        fm = self.ft.fm
        if sdfs_file_name in fm:
            target_ids = fm[sdfs_file_name]['replicas']
            version = fm[sdfs_file_name]['version']
        else:
            target_ids = set(random.sample(list(self.lives), NUM_REPLICAS))
            version = 0
        v_file_name = sdfs_file_name + ',' + str(version)  # update its version: foo.txt -> foo.txt,2
        print('[INFO] Put file %s to %s' % (local_file_name, v_file_name))
        for id in target_ids:
            target_host = self.get_host_from_id(id)
            prefix = 'wenhans2' + '@' + target_host
            p = subprocess.Popen(['scp', local_file_name, prefix + ':' + os.path.join(SDFS_PATH, v_file_name)])
            # os.waitpid(p.pid, 0)
        # print('[INFO] PUT transmission done.')
        self.ft.insert_file(sdfs_file_name, target_ids)

        # multicast udpate message
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            update_msg = {
                'type': 'update',
                'file_name': sdfs_file_name,
                'replicas': list(target_ids),
                'version': fm[sdfs_file_name]['version'],
            }
            for host in ALL_HOSTS:
                s.sendto(json.dumps(update_msg).encode('utf-8'), (host, self.port))

    def get_file(self, sdfs_file_name, local_file_name, num_version=None):
        """
        Get a file from SDFS, store it to local file system as unix-like path.
        :param sdfs_file_name: e.g. foo.sdfs (flatten system)
        :param local_file_name: e.g. /foo/bar/a.txt
        :param num_version: default is None, to get last updated versions of file
        :return: None
        """
        fm = self.ft.fm
        if sdfs_file_name not in fm:
            print('[ERROR] No such sdfs file: %s' % sdfs_file_name)
            return
        from_id = list(fm[sdfs_file_name]['replicas'])[0]
        v = fm[sdfs_file_name]['version']
        # to get last updated version by default (command get)
        if not num_version:
            version = v - 1
            v_file_name = sdfs_file_name + ',' + str(version)
            prefix = 'wenhans2' + '@' + self.get_host_from_id(from_id)
            print('[INFO] Get file %s from chosen replica %d' % (v_file_name, from_id))
            p = subprocess.Popen(['scp', prefix + ':' + os.path.join(SDFS_PATH, v_file_name), local_file_name])
            # os.waitpid(p.pid, 0)
        # to get several updated version (command get-versions)
        else:
            if num_version > v:
                print('[ERROR] Only %d versions available, request %d.' % (v, num_version))
                return
            # download specific updated verisons and merge to a local file with version mark
            with open(local_file_name, 'a') as af:
                for i in range(v - 1, v - 1 - num_version, -1):
                    prefix = 'wenhans2' + '@' + self.get_host_from_id(from_id)
                    v_file_name = sdfs_file_name + ',' + str(i)
                    p = subprocess.Popen(['scp', prefix + ':' + os.path.join(SDFS_PATH, v_file_name), v_file_name])
                    os.waitpid(p.pid, 0)
                    af.write('\n### version %d\n' % i)
                    with open(v_file_name, 'r') as rf:
                        af.writelines(rf.readlines())
                    os.remove(v_file_name)

    def delete_file(self, sdfs_file_name):
        """
        Delete a file (with its all versions) from SDFS.
        :param sdfs_file_name: e.g. foo.sdfs (flatten system)
        :return:
        """
        fm = self.ft.fm
        if sdfs_file_name not in fm:
            print('[ERROR] No such sdfs file: %s' % sdfs_file_name)
            return
        self.ft.delete_file(sdfs_file_name)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            delete_msg = {
                'type': 'delete',
                'file_name': sdfs_file_name,
            }
            for host in ALL_HOSTS:
                s.sendto(json.dumps(delete_msg).encode('utf-8'), (host, self.port))

    def list_sdfs_file(self, sdfs_file_name):
        fm = self.ft.fm
        if sdfs_file_name not in fm:
            print('[ERROR] No such sdfs file: %s' % sdfs_file_name)
            return
        pprint(fm[sdfs_file_name])

    def show_store(self):
        pprint(self.ft.idm)

    # def monitor(self):
    #     helper = '''
    #     ======  Command List  ======
    #     - get [sdfs_file_name] [local_file_name]
    #     - get-versions [sdfs_file_name] [num-versions] [local_file_name]
    #     - put [local_file_name] [sdfs_file_name]
    #     - delete [sdfs_file_name]
    #     - ls [sdfs_file_name]
    #     - store
    #     - ml
    #     - join
    #     - leave
    #     - lives
    #     ============================
    #     '''
    #     print(helper)
    #     while True:
    #         arg = input('-->')
    #         args = arg.split(' ')
    #         if arg == '?' or arg == 'help':
    #             print(helper)
    #         elif arg.startswith('get-versions'):
    #             if len(args) != 4:
    #                 print('[ERROR FORMAT] get-versions sdfs_file_name num-versions local_file_name')
    #                 continue
    #             self.get_file(args[1], args[3], num_version=int(args[2]))
    #         elif arg.startswith('get'):
    #             if len(args) != 3:
    #                 print('[ERROR FORMAT] get sdfs_file_name local_file_name')
    #                 continue
    #             self.get_file(args[1], args[2])
    #         elif arg.startswith('put'):
    #             if len(args) != 3:
    #                 print('[ERROR FORMAT] put local_file_name sdfs_file_name')
    #                 continue
    #             self.put_file(args[1], args[2])
    #         elif arg.startswith('delete'):
    #             if len(args) != 2:
    #                 print('[ERROR FORMAT] delete sdfs_file_name')
    #                 continue
    #             self.delete_file(args[1])
    #         elif arg.startswith('ls'):
    #             if len(args) != 2:
    #                 print('[ERROR FORMAT] ls sdfs_file_name')
    #                 continue
    #             self.list_sdfs_file(args[1])
    #         elif arg.startswith('store'):
    #             self.show_store()
    #         elif arg == 'fm':
    #             pprint(self.ft.fm)
    #         elif arg == 'idm':
    #             pprint(self.ft.idm)
    #         elif arg == 'join':
    #             self.failure_detector.join()
    #         elif arg == 'leave':
    #             self.failure_detector.leave()
    #         elif arg == 'ml':
    #             self.failure_detector.print_ml()
    #         elif arg == 'lives':
    #             print(self.lives)
    #         else:
    #             print('[ERROR] Invalid input arg %s' % arg)

    # def run(self):
    #     # init default SDFS path
    #     if os.path.exists(SDFS_PATH):
    #         shutil.rmtree(SDFS_PATH)
    #     os.mkdir(SDFS_PATH)
    #     t_receiver = threading.Thread(target=self.receiver)
    #     t_receiver.start()


# def main():
#     s = Server(host=socket.gethostname(), port=DEFAULT_SDFS_PORT)
#     s.run()
#
#
# if __name__ == '__main__':
#     main()
