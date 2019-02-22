import socket
import json
import sys
import time
import threading
import os

# hard code hosts' (VMs') ip and port here
# todo: use config file instead
HOSTS = [
    'fa18-cs425-g33-01.cs.illinois.edu',  # 01
    'fa18-cs425-g33-02.cs.illinois.edu',  # 02
    'fa18-cs425-g33-03.cs.illinois.edu',  # 03
    'fa18-cs425-g33-04.cs.illinois.edu',  # 04
    'fa18-cs425-g33-05.cs.illinois.edu',  # 05
    'fa18-cs425-g33-06.cs.illinois.edu',  # 06
    'fa18-cs425-g33-07.cs.illinois.edu',  # 07
    'fa18-cs425-g33-08.cs.illinois.edu',  # 08
    'fa18-cs425-g33-09.cs.illinois.edu',  # 09
    'fa18-cs425-g33-10.cs.illinois.edu',  # 10
]
PORT = 55558


class QueryThread(threading.Thread):
    def __init__(self, pattern, host, port):
        """
        Define thread for query.

        :param pattern: query string, e.g. 'a'(raw string), 'a[a-z]b'(regex)
        :param host: host of query target
        :param port: port of query target
        """
        super(QueryThread, self).__init__()
        self.pattern = pattern
        self.host = host
        self.port = port
        self.time_cost = -1.0  # record time cost for single thread

    def run(self):
        """
        Do the query as a single thread for a client.
        :return: None
        """
        logs = []  # the result of query
        d = {
            'pattern': self.pattern,
        }  # pattern json format

        # do the query for each host
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                t_start = time.time()
                s.connect((self.host, self.port))

                # send query pattern as json format
                data = json.dumps(d).encode('utf-8')
                s.sendall(data)

                # receive return results
                while True:
                    data = b''
                    # declare the barrier here
                    # only process after receiving all returned data
                    while True:
                        temp = s.recv(4096)
                        if temp:
                            data += temp
                        else:
                            break
                    if data:
                        res = json.loads(data.decode('utf-8'))
                        logs += res
                    else:
                        break

                if logs:
                    with open('%s.temp' % self.host, 'w') as f:
                        for log in logs:
                            line = ' '.join([log.get('host', '#'),
                                             log.get('port', '#'),
                                             log.get('log_path', '#'),
                                             str(log.get('line_number', -1)),
                                             log.get('content', '#')])
                            f.write(line)
                t_end = time.time()
                self.time_cost = t_end - t_start

            # handle the client exception
            except (OSError, socket.error) as e:
                print('[ERROR]: ', self.host, e.__class__().__str__(), e.__str__())


class Client:
    def __init__(self, hosts=HOSTS, port=PORT):
        self.hosts = hosts
        self.port = port

    def clean_temp_files(self):
        for file in os.listdir(os.path.dirname(os.path.realpath(__file__))):
            if file.endswith('.temp'):
                print('[INFO] Old temp file %s is founded and cleaned.' % file)
                os.remove(file)

    def query(self, pattern):
        """
        Do the query as a client. Kill the client after finishing the query.

        :param pattern: query string, e.g. 'a'(raw string), 'a[a-z]b'(regex)
        :return: None
        """

        self.clean_temp_files()  # clean .temp files

        time_start = time.time()  # record total parallel time
        d_time = {}  # record time cost for each thread

        # assert worker for each query
        workers = [QueryThread(pattern, host, PORT) for host in HOSTS]
        for worker in workers:
            worker.start()

        # end each worker, record time cost
        for worker in workers:
            worker.join()
            d_time[worker.host] = worker.time_cost

        time_end = time.time()  # # record total parallel time

        # get results from local-saved .temp files
        d_cnt = {}
        for file in os.listdir(os.path.dirname(os.path.realpath(__file__))):
            cnt = 0
            if file.endswith('.temp'):
                with open(file, 'r') as f:
                    for line in f:
                        print(line, end='')
                        cnt += 1
                d_cnt[file.rstrip('.temp')] = cnt

        print('===== STAT =====')
        total_lines = 0
        for host, cnt in d_cnt.items():
            print('From %s, %d lines matched, used %.4f secs.' % (host, cnt, d_time.get(host, -1.)))
            total_lines += cnt
        print('Total %d line, used %.4f secs.' % (total_lines, time_end - time_start))


if __name__ == '__main__':
    c = Client()
    if len(sys.argv) != 2:
        print('[ERROR]: Input arg should be 1 for regex.')
    else:
        pattern = sys.argv[1]
        c.query(pattern=pattern)
