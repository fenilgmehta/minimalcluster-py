import datetime
import multiprocessing
import os
import sys
import time
from multiprocessing.managers import SyncManager
import socket

if sys.version_info.major == 3:
    import queue as Queue
else:
    import Queue

__all__ = ['WorkerNode']

# -*- coding: future_fstrings -*-
# use `pip install future-fstrings`

# Higher the DEBUG_LOG value, higher the debug statements printed
# DEBUG_LOG = 0, no log messages
#           = 1, basic messages when task begins, task finishes and other messages related to connection with the master node
#           = 2, used when reporting bug to the developer
DEBUG_LOG = multiprocessing.Value('i', 1)


def set_debug_level(debug_level_n):
    global DEBUG_LOG
    with DEBUG_LOG.get_lock():
        DEBUG_LOG.value = debug_level_n


def get_debug_level():
    global DEBUG_LOG
    return DEBUG_LOG.value


def print_debug(msg="", end="\n", file=sys.stderr, level=0):
    global DEBUG_LOG
    if get_debug_level() >= level:
        print(msg, end=end, file=file)


def single_worker(envir, fun, job_q, result_q, error_q, history_d, hostname):
    """ A worker function to be launched in a separate process. Takes jobs from
        job_q - each job a list of numbers to factorize. When the job is done,
        the result (dict mapping number -> list of factors) is placed into
        result_q. Runs until job_q is empty.
    """

    # Reference:
    #https://stackoverflow.com/questions/4484872/why-doesnt-exec-work-in-a-function-with-a-subfunction
    exec(envir) in locals()
    globals().update(locals())
    while True:
        try:
            job_id, job_detail = job_q.get_nowait()
            # history_q.put({job_id: hostname})
            history_d[job_id] = hostname
            outdict = {n: globals()[fun](n) for n in job_detail}
            result_q.put((job_id, outdict))
        except Queue.Empty:
            os.kill(os.getpid(), 9)
            return
        except:
            # send the Unexpected error to master node
            error_q.put("Worker Node '{}': ".format(hostname) + "; ".join([repr(e) for e in sys.exc_info()]))
            os.kill(os.getpid(), 9)
            return

def mp_apply(envir, fun, shared_job_q, shared_result_q, shared_error_q, shared_history_d, hostname, nprocs):
    """ Split the work with jobs in shared_job_q and results in
        shared_result_q into several processes. Launch each process with
        single_worker as the worker function, and wait until all are
        finished.
    """
    
    procs = []
    for i in range(nprocs):
        p = multiprocessing.Process(
                target=single_worker,
                args=(envir, fun, shared_job_q, shared_result_q, shared_error_q, shared_history_d, hostname))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()

# this function is put at top level rather than as a method of WorkerNode class
# this is to bypass the error "AttributeError: type object 'ServerQueueManager' has no attribute 'from_address'""
def heartbeat(queue_of_worker_list, worker_hostname, nprocs, status):
    """
    heartbeat will keep an eye on whether the master node is checking the list of valid nodes
    if it detects the signal, it will share the information of current node with the master node.
    """
    while True:
        if not queue_of_worker_list.empty():
            queue_of_worker_list.put((worker_hostname, nprocs, os.getpid(), status.value))
        time.sleep(0.01)


def get_network_ip():
    try:
        return [
            l for l in (
                [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
                [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]
            ) if l
        ][0][0]
    except OSError as e:
        print(f"OSError: {e}")
        print(f"WARNING: returning loopback IP address: 127.0.0.1")
        return "127.0.0.1"


class WorkerNode:

    def __init__(self, IP, PORT, AUTHKEY, nprocs, debug_level=2, limit_nprocs_to_cpucores=True):
        """
        Method to initiate a master node object.

        IP: the hostname or IP address of the Master Node
        PORT: the port to use (decided by Master NOde)
        AUTHKEY: The process's authentication key (a string or byte string).
                 It can't be None for Worker Nodes.
        nprocs: Integer. The number of processors on the Worker Node to be available to the Master Node.
                It should be less or equal to the number of processors on the Worker Node. If higher than that, the # of available processors will be used instead.
        debug_level: Integer. The level of debug statements to be print to the error stream. Higher the number, more the debug statements.
                     Levels = [0, 1, 2, 3]
                     0 -> only important messages
                     1 -> above messages and task execution related messages
                     2 -> above messages and connection related messages
                     3 -> above messages, spawned process and execution job/work related messages
        limit_nprocs_to_CPU: bool. Flag which decides the maximum number of processes that can be spawn.
                             If True, then the max sub-processes is <= # of available processors
                             If False, then any number of sub-processes can be created, there is no limitation.
                             However, note that for compute intensive tasks, performance can be impacted.
        """
        assert type(AUTHKEY) in [str, bytes], "AUTHKEY must be either string or byte string."
        assert type(nprocs) == int, "'nprocs' must be an integer."
        assert type(debug_level) == int, "'debug_level' must be an integer."
        assert type(limit_nprocs_to_cpucores) == bool, "'limit_nprocs_to_cpucores' must be an boolean."

        self.IP = IP
        self.PORT = PORT
        self.AUTHKEY = AUTHKEY.encode() if type(AUTHKEY) == str else AUTHKEY
        N_local_cores = multiprocessing.cpu_count()
        if limit_nprocs_to_cpucores and nprocs > N_local_cores:
            print_debug("[WARNING] nprocs specified is more than the # of cores of this node. Using the # of cores ({}) instead.".format(N_local_cores), level=0)
            self.nprocs = N_local_cores
        elif nprocs < 1:
            print_debug("[WARNING] nprocs specified is not valid. Using the # of cores ({}) instead.".format(N_local_cores), level=0)
            self.nprocs = N_local_cores
        else:
            self.nprocs = nprocs
        self.connected = False
        self.worker_hostname = "{}/{}".format(socket.getfqdn(), get_network_ip())
        set_debug_level(debug_level_n=debug_level)
        self.working_status = multiprocessing.Value("i", 0)  # if the node is working on any work loads

    def connect(self):
        """
        Connect to Master Node after the Worker Node is initialized.
        """
        class ServerQueueManager(SyncManager):
            pass

        ServerQueueManager.register('get_job_q')
        ServerQueueManager.register('get_result_q')
        ServerQueueManager.register('get_error_q')
        ServerQueueManager.register('get_envir')
        ServerQueueManager.register('target_function')
        ServerQueueManager.register('queue_of_worker_list')
        ServerQueueManager.register('dict_of_job_history')

        self.manager = ServerQueueManager(address=(self.IP, self.PORT), authkey=self.AUTHKEY)

        try:
            if not self.quiet:
                print_debug('[{}] Building connection to {}:{}'.format(str(datetime.datetime.now()), self.IP, self.PORT), level=1)
            self.manager.connect()
            if not self.quiet:
                print_debug('[{}] Client connected to {}:{}'.format(str(datetime.datetime.now()), self.IP, self.PORT), level=1)
            self.connected = True
            self.job_q = self.manager.get_job_q()
            self.result_q = self.manager.get_result_q()
            self.error_q = self.manager.get_error_q()
            self.envir_to_use = self.manager.get_envir()
            self.target_func = self.manager.target_function()
            self.queue_of_worker_list = self.manager.queue_of_worker_list()
            self.dict_of_job_history = self.manager.dict_of_job_history()
        except:
            print_debug("[ERROR] No connection could be made. Please check the network or your configuration.", level=1)

    def join_cluster(self):
        """
        This method will connect the worker node with the master node, and start to listen to the master node for any job assignment.
        """

        self.connect()

        if self.connected:

            # start the `heartbeat` process so that the master node can always know if this node is still connected.
            self.heartbeat_process = multiprocessing.Process(target=heartbeat, args=(self.queue_of_worker_list, self.worker_hostname, self.nprocs, self.working_status,))
            self.heartbeat_process.start()

            if not self.quiet:
                print_debug('[{}] Listening to Master node {}:{}'.format(str(datetime.datetime.now()), self.IP, self.PORT), level=1)

            while True:

                try:
                    if_job_q_empty = self.job_q.empty()
                except EOFError:
                    print("[{}] Lost connection with Master node.".format(str(datetime.datetime.now())))
                    sys.exit(1)

                if not if_job_q_empty and self.error_q.empty():
                    print_debug("[{}] Started working on some tasks.".format(str(datetime.datetime.now())), level=1)

                    print("[{}] Started working on some tasks.".format(str(datetime.datetime.now())))


                    # load environment setup
                    try:
                        envir = self.envir_to_use.get(timeout = 3)
                        self.envir_to_use.put(envir)
                    except:
                        sys.exit("[ERROR] Failed to get the environment statement from Master node.")

                    # load task function
                    try:
                        target_func = self.target_func.get(timeout = 3)
                        self.target_func.put(target_func)
                    except:
                        sys.exit("[ERROR] Failed to get the task function from Master node.")
                    
                    self.working_status.value = 1
                    mp_apply(envir, target_func, self.job_q, self.result_q, self.error_q, self.dict_of_job_history, self.worker_hostname, self.nprocs)
                    print("[{}] Tasks finished.".format(str(datetime.datetime.now())))
                    print_debug("[{}] Tasks finished.".format(str(datetime.datetime.now())), level=1)
                    self.working_status.value = 0

                time.sleep(0.1) # avoid too frequent communication which is unnecessary
