# Echo server program
import socket
import json
import re
import os


# hard code hosts' (VMs') ip and port here
# todo: use config file instead
HOST = socket.gethostname()
PORT = 55558


class Server:
    def __init__(self, host=HOST, port=PORT):
        """
        Server initialization.
        Make sure the server has already known the .log file.

        :param host: server host
        :param port: server post
        """
        self.host = host
        self.port = port
        self.log_path = ''
        root = '/home/wenhans2'
        for file in os.listdir(root):
            if file.endswith('.log'):
                self.log_path = os.path.join(root, file)

    def run(self):
        """
        Run a server, to receive the query pattern from clients and return log data.

        :return: None
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen(1)
            while True:
                print('[INFO]: Waiting for connection ...')
                conn, addr = s.accept()
                with conn:
                    print('[INFO]: Connected by', addr)

                    # receive the pattern and process data
                    try:
                        data = conn.recv(1024)
                        if data:
                            pattern = json.loads(data.decode('utf-8'))['pattern']
                            cnt = 0  # line number counter
                            buffer = []  # store all the matched results
                            with open(self.log_path, 'r') as f:
                                for line in f:
                                    cnt += 1
                                    if re.search(pattern, line):
                                        buffer.append({
                                            'log_path': self.log_path,
                                            'host': self.host,
                                            'port': str(self.port),
                                            'line_number': cnt,
                                            'content': line,
                                        })  # json format for returning the matched log results
                            # return the results to the client
                            if buffer:
                                data = json.dumps(buffer).encode('utf-8')
                                conn.sendall(data)

                    except Exception as e:
                        print('[ERROR]:', e.__str__())


if __name__ == '__main__':
    s = Server()
    s.run()



