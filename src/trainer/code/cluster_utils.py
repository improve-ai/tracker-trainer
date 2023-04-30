import os
import socket
from subprocess import Popen
import time

from config import CORES_PER_WORKER


def get_ip_from_host(host_name) -> str:
    """
    Gets ip using a hostname

    Parameters
    ----------
    host_name: str
        hostname for which ip will be extracted

    Returns
    -------
    str
        ip for desired hostname

    """
    ip_wait_time = 200
    counter = 0
    ip = ""

    while counter < ip_wait_time and ip == "":
        try:
            ip = socket.gethostbyname(host_name)
            break
        except Exception as exc:
            counter += 1
            time.sleep(1)

    if counter == ip_wait_time and ip == "":
        raise Exception(f'Exceeded max wait time of {ip_wait_time}s for hostname resolution')

    return ip


def start_daemons(
        master_ip: str, is_master: bool, dask_pth: str = None):
    """
    Start dask daemons (both scheduler and workers) using CLI call from python

    Parameters
    ----------
    master_ip: str
        master node ip
    is_master: bool
        is caller a master node
    dask_pth: str
        path to dask

    Returns
    -------
    None

    """

    cmd_start_scheduler = \
        'dask-scheduler' if not dask_pth \
        else os.path.join(dask_pth, 'dask-scheduler')

    cmd_start_worker = \
        'dask-worker' if not dask_pth \
        else os.path.join(dask_pth, 'dask-worker')
    # following https://github.com/dask/distributed/issues/7523 it might be
    # beneficial to spawn 1 worker for 4 cores available

    # dask-worker scheduler:8786 --nprocs 3 --nthreads 2 --resources "process=1"
    # res_cmd = ['--nprocs', '1', '--nthreads', '2', '--memory-limit=4e9']

    #
    # WARN: BE CAREFUL ABOUT INCREASING -nprocs FROM 1.  RESOURCES ARE PER
    # PROCESS AND WE USE RESOURCES TO SINGLE THREAD ACCESS TO THE INPUT PIPE
    #

    # Each worker has 1 records pipe resource to ensure single threaded access
    res_cmd = ['--death-timeout', '10']

    # TODO using multiple workers per node seems to speed things up
    #  (maybe this would also solve the work-stealing bug?)
    # n_worker_processes = 1
    n_worker_processes = 1 if os.cpu_count() // CORES_PER_WORKER == 0 else os.cpu_count() // CORES_PER_WORKER

    if n_worker_processes > 1:
        res_cmd += ['--nworkers', str(n_worker_processes)]

    schedule_conn_string = f'tcp://{master_ip}:8786'

    scheduler_process = None
    worker_process = None

    try:
        if is_master:
            print('starting master')
            scheduler_process = Popen([cmd_start_scheduler])

        time.sleep(1)
        print('starting worker')
        print(f'Start worker CMD: {res_cmd}')
        worker_process = \
            Popen([cmd_start_worker, schedule_conn_string] + res_cmd)
    except Exception as exc:
        print(exc)

    return scheduler_process, worker_process, n_worker_processes
