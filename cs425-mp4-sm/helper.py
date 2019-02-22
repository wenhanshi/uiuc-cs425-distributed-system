class Status:
    JOINING = 'JOINING'
    RUNNING = 'RUNNING'
    FAILED = 'FAILED'
    LEAVED = 'LEAVED'


def get_id_from_host(host):
    """
    e.g. fa18-cs425-g33-01.cs.illinois.edu -> 1
    :param host: host name
    :return: an integer id
    """
    return int(host.split('.')[0].split('-')[-1])


def get_host_from_id(host_id):
    """
    e.g. 1 -> fa18-cs425-g33-01.cs.illinois.edu
    :param host_id: int id
    :return: host str
    """
    return 'fa18-cs425-g33-%02d.cs.illinois.edu' % host_id
