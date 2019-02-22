
# time stamp format in this program
TIME_FORMAT_STRING = '%H:%M:%S'

# static introducer addr
INTRODUCER_HOST = 'fa18-cs425-g33-01.cs.illinois.edu'

# made for introducer
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

# default socket port
DEFAULT_PORT = 55343

# topology of 10 VMs
# {source: [dest_1, dest_2, ...]}
CONNECTIONS = {
    'fa18-cs425-g33-01.cs.illinois.edu': [
        'fa18-cs425-g33-09.cs.illinois.edu',
        'fa18-cs425-g33-10.cs.illinois.edu',
        'fa18-cs425-g33-02.cs.illinois.edu',
        'fa18-cs425-g33-03.cs.illinois.edu',
    ],
    'fa18-cs425-g33-02.cs.illinois.edu': [
        'fa18-cs425-g33-03.cs.illinois.edu',
        'fa18-cs425-g33-04.cs.illinois.edu',
        'fa18-cs425-g33-01.cs.illinois.edu',
        'fa18-cs425-g33-10.cs.illinois.edu',
    ],
    'fa18-cs425-g33-03.cs.illinois.edu': [
        'fa18-cs425-g33-04.cs.illinois.edu',
        'fa18-cs425-g33-05.cs.illinois.edu',
        'fa18-cs425-g33-01.cs.illinois.edu',
        'fa18-cs425-g33-02.cs.illinois.edu',
    ],
    'fa18-cs425-g33-04.cs.illinois.edu': [
        'fa18-cs425-g33-05.cs.illinois.edu',
        'fa18-cs425-g33-06.cs.illinois.edu',
        'fa18-cs425-g33-02.cs.illinois.edu',
        'fa18-cs425-g33-03.cs.illinois.edu',
    ],
    'fa18-cs425-g33-05.cs.illinois.edu': [
        'fa18-cs425-g33-06.cs.illinois.edu',
        'fa18-cs425-g33-07.cs.illinois.edu',
        'fa18-cs425-g33-03.cs.illinois.edu',
        'fa18-cs425-g33-04.cs.illinois.edu',
    ],
    'fa18-cs425-g33-06.cs.illinois.edu': [
        'fa18-cs425-g33-07.cs.illinois.edu',
        'fa18-cs425-g33-08.cs.illinois.edu',
        'fa18-cs425-g33-04.cs.illinois.edu',
        'fa18-cs425-g33-05.cs.illinois.edu',
    ],
    'fa18-cs425-g33-07.cs.illinois.edu': [
        'fa18-cs425-g33-08.cs.illinois.edu',
        'fa18-cs425-g33-09.cs.illinois.edu',
        'fa18-cs425-g33-05.cs.illinois.edu',
        'fa18-cs425-g33-06.cs.illinois.edu',
    ],
    'fa18-cs425-g33-08.cs.illinois.edu': [
        'fa18-cs425-g33-09.cs.illinois.edu',
        'fa18-cs425-g33-10.cs.illinois.edu',
        'fa18-cs425-g33-06.cs.illinois.edu',
        'fa18-cs425-g33-07.cs.illinois.edu',
    ],
    'fa18-cs425-g33-09.cs.illinois.edu': [
        'fa18-cs425-g33-10.cs.illinois.edu',
        'fa18-cs425-g33-01.cs.illinois.edu',
        'fa18-cs425-g33-07.cs.illinois.edu',
        'fa18-cs425-g33-08.cs.illinois.edu',
    ],
    'fa18-cs425-g33-10.cs.illinois.edu': [
        'fa18-cs425-g33-01.cs.illinois.edu',
        'fa18-cs425-g33-02.cs.illinois.edu',
        'fa18-cs425-g33-08.cs.illinois.edu',
        'fa18-cs425-g33-09.cs.illinois.edu',
    ]
}
