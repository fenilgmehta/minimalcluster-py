import datetime
import multiprocessing
import os
import socket
import sys
import time
from multiprocessing.managers import SyncManager

if sys.version_info.major == 3:
    import queue as Queue
else:
    import Queue

__all__ = ['WorkerNode', 'set_debug_level', 'get_debug_level']

# -*- coding: future_fstrings -*-
# use `pip install future-fstrings`

# Higher the DEBUG_LOG value, higher the debug statements printed
# DEBUG_LOG = 0, no log messages
#           = 1, basic messages when task begins, task finishes and other
#           = 2, messages related to connection with the master node
#           = 3, used when reporting bug to the developer
DEBUG_LOG = multiprocessing.Value('i', 2)


def set_debug_level(debug_level_n):
    """
    Will update the debugging log level to debug_level_n

    NOTE: debugging log level is global to the library and
    hence will affect all the object instances created

    :param debug_level_n: int.
    :return: None
    """
    global DEBUG_LOG
    with DEBUG_LOG.get_lock():
        DEBUG_LOG.value = debug_level_n


def get_debug_level():
    """
    Will return the current the debugging log level

    NOTE: debugging log level is global to the library and
    hence will affect all the object instances created

    :return: int
    """
    global DEBUG_LOG
    return DEBUG_LOG.value


def print_debug(msg="", end="\n", file=sys.stderr, level=0):
    global DEBUG_LOG
    if get_debug_level() >= level:
        print(msg, end=end, file=file)


def single_worker(envir, fun, job_q, result_q, error_q, history_d, hostname):
    """
    A worker function to be launched in a separate process. Takes jobs from
    job_q - each job a list of numbers to factorize. When the job is done,
    the result (dict mapping number -> list of factors) is placed into
    result_q. Runs until job_q is empty.

    :return: None
    """

    # Reference:
    # https://stackoverflow.com/questions/4484872/why-doesnt-exec-work-in-a-function-with-a-subfunction
    exec(envir) in locals()
    globals().update(locals())
    job_id = job_detail = None  # NEW: 20191209T2203
    while True:
        try:
            job_id, job_detail = job_q.get_nowait()
            # job_id, job_detail = job_q.get(block=True, timeout=2.94)  # NEW: to test: 20191209T2203
            print_debug(f"DEBUG: [{str(os.getppid()):5} -> {str(os.getpid()):5}] to work on: {job_id}", level=3)
            # history_q.put({job_id: hostname})
            history_d[job_id] = hostname
            outdict = {n: globals()[fun](n) for n in job_detail}
            result_q.put((job_id, outdict))
            print_debug(f"DEBUG: [{str(os.getppid()):5} -> {str(os.getpid()):5}] work finished: {job_id}", level=3)
        except EOFError:
            print_debug(f"\n[ERROR] [{str(os.getppid()):5} -> {str(os.getpid()):5}] Connection closed by the server. STOPPING the spawned processes...", level=1)
            break
        except Queue.Empty:
            print_debug(f"\nDEBUG: [{str(os.getppid()):5} -> {str(os.getpid()):5}] Queue.Empty: returning from single_worker(...)", level=3)
            break
        except Exception as e:
            # Send the Unexpected error to master node. This will not stop the master node, it will continue to execute
            error_q.put("Worker Node '{}': ".format(hostname) + "; ".join([repr(e) for e in sys.exc_info()]))
            error_q.put(f"#Error#{hostname}#{job_id}#{len(job_detail)}")
            print_debug(f"\nDEBUG: [{str(os.getppid()):5} -> {str(os.getpid()):5}] Unknown error, returning from single_worker(...)\n\t{e}", level=3)
            break

    os.kill(os.getpid(), 9)
    # exit(0)  # This does not stop the spawned process on remote worker node
    # sys.exit()  # This does not stop the spawned process on remote worker node
    print_debug(f"DEBUG: using return: STOPPING single_worker(...) {os.getpid()}", level=3)
    return


def mp_apply(envir, fun,
             shared_job_q, shared_result_q, shared_error_q, shared_history_d, hostname, nprocs,
             heartbeat_process, approx_max_job_time):
    """
    Split the work with jobs in shared_job_q and results in
    shared_result_q into several processes. Launch each process with
    single_worker as the worker function, and wait until all are
    finished.
    :return: None
    """

    def spawn_new_process():
        p = multiprocessing.Process(
            target=single_worker,
            args=(envir, fun, shared_job_q, shared_result_q, shared_error_q, shared_history_d, hostname),
            daemon=False  # this causes the sub-processes to exit as soon as the parent process exits/killed
        )
        p.start()
        return p

    procs = []
    for i in range(nprocs):
        procs.append(spawn_new_process())

    last_process_count_change = float(2 ** 128)
    last_active_processes = 0
    bool_changed = False
    i_iter = 0
    # while i_iter < len(procs):
    #     procs[i_iter].join()
    while True:
        if get_debug_level() >= 3:
            print_debug(f"DEBUG: shared_job_q.empty() = {shared_job_q.empty()}", level=3)
            for i in procs:
                print_debug(f"DEBUG: process [{i.pid}] : is_alive()={i.is_alive()}, exitcode={i.exitcode}", level=3)

        if not heartbeat_process.is_alive():
            for i in procs:
                i.kill()
            return False

        active_processes = sum([p_i.is_alive() for p_i in procs])

        # if all spawned processes have exited, exit the while loop and return
        if active_processes == 0:
            break

        # ADDED - 20191206 - start
        if (not bool_changed) and len(procs) > nprocs and active_processes != nprocs:
            bool_changed = True
            last_process_count_change = time.time()
            last_active_processes = active_processes
        print_debug(f"val bool_changed = {bool_changed}", level=3)
        print_debug(f"Time since last process count change = {(time.time() - last_process_count_change):5.2f}", level=3)
        if bool_changed:
            if last_active_processes != active_processes:
                last_process_count_change = time.time()
                last_active_processes = active_processes
            elif (time.time() - last_process_count_change) > approx_max_job_time:
                print_debug(
                    f"\nval last_active_processes = {last_active_processes}"
                    f"\nval active_processes = {active_processes}"
                    f"\nTime since last process count change = {(time.time() - last_process_count_change):5.2f}"
                    "\n\n*************************************************************************"
                    "\nDEBUG: CRITICAL WARNING processes seem to do no work. Hence stopping them",
                    "\n*************************************************************************", level=2
                )
                # heartbeat_process.kill()
                for i in procs: i.kill()
                time.sleep(0.5)
                return False
        # ADDED - 20191206 - end ^

        # WARNING - it may creates problem
        # if there is work in `shared_job_q` and spawned process has exited, then create a new process
        if active_processes < nprocs and len(procs) <= int(1.5 * nprocs) and (not shared_job_q.empty()):
            procs.append(spawn_new_process())

        time.sleep(2.88)
        i_iter += 1

    print_debug(f"\n\n################################################################## DEBUG: mp_apply returned :)\n\n", level=3)
    return True


# this function is put at top level rather than as a method of WorkerNode class
# this is to bypass the error "AttributeError: type object 'ServerQueueManager' has no attribute 'from_address'""
def heartbeat(queue_of_worker_list, worker_hostname, nprocs, status):
    """
    heartbeat will keep an eye on whether the master node is checking the list of valid nodes
    if it detects the signal, it will share the information of current node with the master node.
    :return: None
    """

    while True:
        try:
            if not queue_of_worker_list.empty():
                queue_of_worker_list.put((worker_hostname, nprocs, os.getpid(), status.value))
                time.sleep(1.01)
            time.sleep(0.05)
        except EOFError:
            print_debug(f"[WARNING] Master node shutdown. Shutting down the worker node...", level=1)
            break
        except (ConnectionResetError, BrokenPipeError) as e:
            print_debug(f"[ERROR] [{os.getpid()}] [type={type(e)}] Connection closed by the server. Closing the connection checker...", level=1)
            break

    print_debug(f"DEBUG: STOPPING {os.getpid()}, heartbeat(...)", level=3)
    os.kill(os.getpid(), 9)
    # exit(0)  # This does not stop the spawned process on remote worker node
    # sys.exit()  # This does not stop the spawned process on remote worker node
    print_debug(f"DEBUG: using return: STOPPING heartbeat(...) process {os.getpid()}", level=3)
    return


def get_network_ip():
    """
    Returns the network IP address of the client machine.
    If not connected to the network, will return loop-back IP address
    :return: str
    """

    try:
        return [
            l for l in (
                [ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
                [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]
            ) if l
        ][0][0]
    except OSError as e:
        print(f"OSError: {e}")
        print(f"WARNING: returning loop-back IP address: 127.0.0.1")
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
            print_debug('[{}] Building connection to {}:{}'.format(str(datetime.datetime.now()), self.IP, self.PORT), level=2)
            self.manager.connect()
            print_debug('[{}] Client connected to {}:{}'.format(str(datetime.datetime.now()), self.IP, self.PORT), level=2)

            self.connected = True
            self.job_q = self.manager.get_job_q()
            self.result_q = self.manager.get_result_q()
            self.error_q = self.manager.get_error_q()
            self.envir_to_use = self.manager.get_envir()
            self.target_func = self.manager.target_function()
            self.queue_of_worker_list = self.manager.queue_of_worker_list()
            self.dict_of_job_history = self.manager.dict_of_job_history()
        except:
            self.connected = False
            print_debug("[ERROR] No connection could be made. Please check the network or your configuration.", level=1)

    def join_cluster(self, approx_max_job_time):
        """
        This method will connect the worker node with the master node, and start to listen to the master node for any job assignment.
        """

        if approx_max_job_time is None or (isinstance(approx_max_job_time, (int, float,)) and approx_max_job_time <= 0):
            approx_max_job_time = 2 ** 30
        elif not (approx_max_job_time is None) and not (isinstance(approx_max_job_time, (int, float,))):
            approx_max_job_time = 2 ** 30

        self.connect()

        if self.connected:
            # start the `heartbeat` process so that the master node can always know if this node is still connected.
            # daemon=True means that this process will be killed when the function returns
            heartbeat_process = multiprocessing.Process(target=heartbeat,
                                                        args=(self.queue_of_worker_list, self.worker_hostname, self.nprocs, self.working_status,),
                                                        daemon=False)
            heartbeat_process.start()

            print_debug('[{}] Listening to Master node {}:{}'.format(str(datetime.datetime.now()), self.IP, self.PORT), level=2)

            while True:
                try:
                    if_job_q_empty = self.job_q.empty()
                except EOFError:
                    raise Exception("[{}] Lost connection with Master node.".format(str(datetime.datetime.now())))
                    # print_debug("[{}] Lost connection with Master node.".format(str(datetime.datetime.now())), level=1)
                    # sys.exit(1)

                if not if_job_q_empty and self.error_q.empty():
                    print_debug("[{}] Started working on some tasks.".format(str(datetime.datetime.now())), level=1)

                    # load environment setup
                    try:
                        envir = self.envir_to_use.get(timeout=3)
                        self.envir_to_use.put(envir)
                    except Exception as e:
                        raise Exception("[ERROR] Failed to get the environment statement from Master node -> {}".format(str(e)))
                        # sys.exit("[ERROR] Failed to get the environment statement from Master node.")

                    # load task function
                    try:
                        target_func = self.target_func.get(timeout=3)
                        self.target_func.put(target_func)
                    except Exception as e:
                        raise Exception("[ERROR] Failed to get the task function from Master node -> {}".format(str(e)))
                        # sys.exit("[ERROR] Failed to get the task function from Master node.")

                    self.working_status.value = 1
                    mp_apply_success = mp_apply(envir, target_func,
                                                self.job_q, self.result_q, self.error_q, self.dict_of_job_history, self.worker_hostname, self.nprocs,
                                                heartbeat_process, approx_max_job_time)
                    print_debug("[{}] Tasks finished.".format(str(datetime.datetime.now())), level=1)
                    self.working_status.value = 0
                    if not mp_apply_success:
                        print_debug("heartbeat_process stopped. Hence returning", level=1)
                        return

                # TODO: check the impact of increase in sleep time
                # avoid too frequent communication which is unnecessary
                # time.sleep(0.1)
                time.sleep(0.53)
