import os
import subprocess

from utils import unify_byte_unit


def cpu_usage():
    child1 = subprocess.Popen(['ps', '-ux'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, shell=False)
    sub_chan = child2.communicate()
    if not sub_chan[0]:
        result = 0.0
    else:
        result = sub_chan[0].split()[2].decode('utf-8')
    return result


def io_read():
    child1 = subprocess.Popen(['pidstat', '-d'], stdout=subprocess.PIPE, shell=False)
    child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE, shell=False)
    sub_chan = child2.communicate()
    if not sub_chan[0]:
        result = 0.0
    else:
        result = sub_chan[0].split()[3].decode('utf-8')
    return result


def io_write():
    child1 = subprocess.Popen(['pidstat', '-d'], stdout=subprocess.PIPE, shell=False)
    child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE, shell=False)
    sub_chan = child2.communicate()
    if not sub_chan[0]:
        result = 0.0
    else:
        result = sub_chan[0].split()[4].decode('utf-8')
    return result


def memory_usage():
    child1 = subprocess.Popen(['ps', '-ux'], stdout=subprocess.PIPE, shell=False)
    child2 = subprocess.Popen(['grep', 'gaussd[b]'], stdin=child1.stdout, stdout=subprocess.PIPE, shell=False)
    sub_chan = child2.communicate()
    if not sub_chan[0]:
        result = 0.0
    else:
        result = sub_chan[0].split()[3].decode('utf-8')
    return result


def disk_space():
    pg_data = os.getenv('PGDATA')
    if pg_data is None:
        raise ValueError('not found PGDATA in environment.')
    else:
        pg_data = os.path.realpath(pg_data)
        child = subprocess.Popen(['du', '-sh', pg_data], stdout=subprocess.PIPE, shell=False)
        sub_chan = child.communicate()
        if sub_chan[1] is not None:
            raise ValueError('error when get disk usage of openGauss: {error}'.
                             format(error=sub_chan[1].decode('utf-8')))
        if not sub_chan[0]:
            result = 0.0
        else:
            result = unify_byte_unit(sub_chan[0].decode('utf-8'))
        return result
