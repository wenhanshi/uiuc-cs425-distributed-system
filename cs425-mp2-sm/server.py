import socket
import threading
import json
import random
import datetime
import time
from glob import *


class MessageField:
    TYPE = 'message_type'
    HOST = 'host'
    PORT = 'port'
    INFO = 'info'
    ID = 'id'


class Status:
    JOINING = 'JOINING'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    LEAVED = 'LEAVED'


class MessageType:
    PING = 'PING'
    ACK = 'ACK'
    JOIN = 'JOIN'
    LEAVE = 'LEAVE'


class MembershipList:
    def __init__(self, host_name, init_id):
        self.d = {
            host_name: {
                'id': init_id,  # rand inited by each join operation
                'status': Status.LEAVED,  # init status, LEAVED is default
                'ts': datetime.datetime.now().strftime(TIME_FORMAT_STRING)  # init time stamp
            }
        }


def get_nbs(host_name):
    """
    Given host_name, return [host_name1, host_name2, ...]

    :param host_name:
    :return: static neightbours
    """
    return CONNECTIONS[host_name]


class Server:
    def __init__(self, host_name, port):
        self.id = random.randint(0, 65535)
        self.ml = MembershipList(host_name=host_name, init_id=self.id)
        self.nbs = get_nbs(host_name)
        self.host = host_name
        self.port = port
        self.addr = (self.host, self.port)
        self.timer = {}
        self.ml_lock = threading.Lock()
        self.checker_lock = threading.Lock()
        self.print_ml()

    def print_ml(self):
        """
        Print Membership List to the terminal.

        :return: None
        """
        mld = self.ml.d
        print('=== MembershipList on %s ===' % self.host)
        for k, v in mld.items():
            print('%s: %d [%s] [%s]' % (k, v['id'], v['status'], v['ts']))
        print('============================')

    def is_introducer(self):
        return self.host == INTRODUCER_HOST

    def receiver(self):
        """
        A server's receiver is respnsible to receive all UDP message including four types:

        - PING: update the membership list and return an ACK to the source host
        - ACK: udpate the membership list only for source host
        - JOIN (only from introducer): acknowledge that some server has been introduced to the group by introducer
        - LEAVE: acknowledge that a node is gonna be leaved

        :return: None
        """
        mld = self.ml.d
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.bind(self.addr)
            while True:
                try:

                    # if current node is leaved, it does not receive any message
                    if mld[self.host]['status'] == Status.LEAVED:
                        continue

                    # UDP receiver
                    data, server = s.recvfrom(4096)
                    if data:
                        msg = json.loads(data.decode('utf-8'))
                        msg_type = msg.get(MessageField.TYPE, '#')
                        if msg_type == '#':
                            print('[ERROR] No message type field in info at receiver.')
                            continue
                        print('[INFO] Receive %s msg from %s:%s.' % (msg[MessageField.TYPE],
                                                                     msg[MessageField.HOST],
                                                                     msg[MessageField.PORT]))
                        from_host = msg[MessageField.HOST]
                        now = datetime.datetime.now().strftime(TIME_FORMAT_STRING)

                        self.ml_lock.acquire()

                        # process PING message
                        if msg_type == MessageType.PING:
                            info = msg[MessageField.INFO]
                            for host in info:
                                if host not in mld:
                                    mld[host] = info[host]  # update new item in ml
                                    continue
                                old_time = datetime.datetime.strptime(mld[host]['ts'], TIME_FORMAT_STRING)
                                new_time = datetime.datetime.strptime(info[host]['ts'], TIME_FORMAT_STRING)
                                if new_time > old_time:
                                    mld[host] = info[host]  # update older item in ml
                            ack_msg = {
                                MessageField.TYPE: MessageType.ACK,
                                MessageField.HOST: self.host,
                                MessageField.PORT: DEFAULT_PORT,
                                MessageField.INFO: mld[self.host]
                            }  # return ACK message
                            s.sendto(json.dumps(ack_msg).encode('utf-8'), (from_host, DEFAULT_PORT))

                        # process ACK message
                        elif msg_type == MessageType.ACK:
                            mld[from_host] = msg[MessageField.INFO]
                            if from_host in self.timer:
                                self.checker_lock.acquire()
                                del self.timer[from_host]  # clean time table
                                self.checker_lock.release()

                        # process JOIN message
                        elif msg_type == MessageType.JOIN:
                            mld[from_host] = msg[MessageField.INFO]
                            mld[from_host]['status'] = Status.JOINING
                            mld[from_host]['ts'] = now
                            if self.is_introducer():  # multicast the join message to all nodes as introducer
                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as ss:
                                    join_msg = {
                                        MessageField.TYPE: MessageType.JOIN,
                                        MessageField.HOST: from_host,
                                        MessageField.PORT: DEFAULT_PORT,
                                        MessageField.INFO: mld[from_host]
                                    }
                                    for host in ALL_HOSTS:
                                        if host != from_host and host != self.host:  # except for source node and itself
                                            ss.sendto(json.dumps(join_msg).encode('utf-8'), (host, DEFAULT_PORT))

                        # process LEAVE message
                        elif msg_type == MessageType.LEAVE:
                            mld[from_host]['status'] = Status.LEAVED
                            mld[from_host]['ts'] = now

                        else:
                            print('[ERROR] Unknown message type in info at receiver.')
                        self.ml_lock.release()

                except Exception as e:
                    print(e)

    def sender(self):
        """
        A UDP sender for a node. It sends PING message to its neighbours and maintain time table for
        handling timeout issue.

        :return: None
        """
        mld = self.ml.d
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            while True:
                try:
                    self.print_ml()
                    time.sleep(1)

                    # if current node is leaved, it does not send any message
                    if mld[self.host]['status'] == Status.LEAVED:
                        continue

                    now = datetime.datetime.now()
                    self.ml_lock.acquire()
                    mld[self.host]['ts'] = now.strftime(TIME_FORMAT_STRING)
                    mld[self.host]['status'] = Status.RUNNING

                    # send PING message to all its neighbours
                    for host in self.nbs:
                        if (host not in mld) or (host in mld) and (mld[host]['status'] == Status.LEAVED):
                            continue  # ignore non-discovered nodes and leaved nodes
                        ping_msg = {
                            MessageField.TYPE: MessageType.PING,
                            MessageField.HOST: self.host,
                            MessageField.PORT: self.port,
                            MessageField.INFO: mld
                        }
                        s.sendto(json.dumps(ping_msg).encode('utf-8'), (host, self.port))

                        # update the timer for checker thread
                        if host in mld and host not in self.timer:
                            self.checker_lock.acquire()
                            self.timer[host] = datetime.datetime.now()
                            self.checker_lock.release()
                    self.ml_lock.release()
                except Exception as e:
                    print(e)

    def checker(self):
        """
        A checker process for a node to check timeout (failure) of its neighbours.

        The algorithm based on time table

        The sender sends PING to foo at 24 logic time stamp.

        host | ts
        =========
        foo  | 24

        If receiver gets the ACK from foo, delete the foo item in time table.
        If checker checks that t_now - ts[foo] > LIMIT_TIME, it throws failed status to its own ml.
        If the node is LEAVED or FAILED, the checker ignores the timeout.

        Note: the new PING will not update time table if foo has been existed, e.g. the node send
              PING to foo at 25 but the time table has 24, we do not update 24 to 25 because the oldest time
              is needed to check timeout.

        :return: None
        """
        mld = self.ml.d
        timer = self.timer
        while True:
            try:
                self.checker_lock.acquire()
                for host in list(timer.keys()):
                    now = datetime.datetime.now()
                    time_delta = now - timer[host]
                    if time_delta.seconds > 2.:  # timeout, update timetable
                        if (host in mld) and (mld[host]['status'] not in {Status.FAILED, Status.LEAVED}):
                            print('[INFO] Timeout for host %s from checker.' % host, time_delta)
                            mld[host]['status'] = Status.FAILED
                            mld[host]['ts'] = now.strftime(TIME_FORMAT_STRING)
                        del timer[host]
                self.checker_lock.release()

            except Exception as e:
                print(e)

    def join(self):
        """
        Action join, it tells the introducer the node will be joined to the group.
        Also, it updates self's status to RUNNING.
        A node can join after its leave.

        :return: None
        """
        mld = self.ml.d
        if self.is_introducer():
            print('[INFO] I\'m introducer!')

        mld[self.host]['status'] = Status.RUNNING
        mld[self.host]['ts'] = datetime.datetime.now().strftime(TIME_FORMAT_STRING)
        mld[self.host]['id'] = random.randint(0, 65535)
        if not self.is_introducer():
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                print('[INFO] Send join msg to introducer.')
                join_msg = {
                    MessageField.TYPE: MessageType.JOIN,
                    MessageField.HOST: self.host,
                    MessageField.PORT: self.port,
                    MessageField.INFO: mld[self.host],
                }
                s.sendto(json.dumps(join_msg).encode('utf-8'), (INTRODUCER_HOST, DEFAULT_PORT))

    def leave(self):
        """
        Action leave, it tells its neighbours that the node will leave and leave the last word (LEAVE message).

        :return:
        """
        mld = self.ml.d
        if mld[self.host]['status'] != Status.RUNNING:
            print('[INFO] Cant leave under the status %s.' % mld[self.host]['status'])
            return
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            print('[INFO] Send leave msg to neighbours.')
            leave_msg = {
                MessageField.TYPE: MessageType.LEAVE,
                MessageField.HOST: self.host,
                MessageField.PORT: self.port,
            }
            for host in self.nbs:
                s.sendto(json.dumps(leave_msg).encode('utf-8'), (host, DEFAULT_PORT))
            mld[self.host]['status'] = Status.LEAVED
        print('[INFO] %s safely LEAVES the group.' % self.host)

    def monitor(self):
        """
        It monitors the input command.

        :return: None
        """
        helper = '''
        ======  Command List  ======
        --> ? / help: Print the command list.
        --> join: Join the group.
        --> leave: Leave the group safely.
        --> ml: Print current membership list of the server.
        --> id: Print current id of the server.
        
        '''
        print(helper)
        while True:
            arg = input('-->')
            if arg == '?' or arg == 'help':
                print(helper)
            elif arg == 'join':
                self.join()
            elif arg == 'leave':
                self.leave()
            elif arg == 'ml':
                self.print_ml()
            elif arg == 'id':
                print(self.id)
            else:
                print('[ERROR] Invalid input arg %s' % arg)

    def run(self):
        """
        Run a server as a node in group but not joined yet.
        There are totally four parallel processes for a single node:
        - receiver: receive all UDP message
        - sender: send PING message
        - monitor: monitor the actions including join, leave, id and ml
        - checker: check the failed events

        :return: None
        """
        t_receiver = threading.Thread(target=self.receiver)
        t_sender = threading.Thread(target=self.sender)
        t_checker = threading.Thread(target=self.checker)
        t_monitor = threading.Thread(target=self.monitor)
        t_receiver.start()
        t_sender.start()
        t_checker.start()
        t_monitor.start()
        t_receiver.join()
        t_sender.join()
        t_checker.join()
        t_monitor.join()


def main():
    s = Server(host_name=socket.gethostname(), port=DEFAULT_PORT)
    s.run()


if __name__ == '__main__':
    main()
