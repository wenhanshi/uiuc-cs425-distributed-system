import os

# INIT_SUPERVISOR_IDS = {3, 4, 5}
INIT_SUPERVISOR_IDS = {3, 4, 5, 6, 7, 8, 9, 10}
DEFAULT_SUPERVISOR_PORT = 56777
DEFAULT_DATA_PORT = 56778
DEFAULT_MASTER_HOST = 'fa18-cs425-g33-01.cs.illinois.edu'
DEFAULT_FD_PORT = 56779
DEFAULT_SDFS_PORT = 56780

ALL_HOSTS = [
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

NUM_REPLICAS = 2
SDFS_PATH = os.path.join(os.path.expanduser('~'), 'files')

INPUT_SLEEP_PERIOD = .0
