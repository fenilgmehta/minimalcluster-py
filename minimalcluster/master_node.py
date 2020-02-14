import datetime
import os
import random
import signal
import string
import sys
import time
from functools import partial
from multiprocessing import Process, cpu_count
from multiprocessing.managers import SyncManager, DictProxy
from socket import getfqdn
from types import FunctionType
import collections

if sys.version_info.major == 3:
    from queue import Queue as _Queue
else:
    from Queue import Queue as _Queue

__all__ = ['MasterNode']

DEBUG_LOG = None


def print_debug(msg="", end="\n", file=sys.stderr, level=0):
    global DEBUG_LOG
    if DEBUG_LOG >= level:
        print(f"SERVER DEBUG: {msg}", end=end, file=file)


# Make Queue.Queue pickleable
# Ref: https://stackoverflow.com/questions/25631266/cant-pickle-class-main-jobqueuemanager
class Queue(_Queue):
    """ A picklable queue. """

    def __getstate__(self):
        # Only pickle the state we care about
        return self.maxsize, self.queue, self.unfinished_tasks

    def __setstate__(self, state):
        # Re-initialize the object, then overwrite the default state with
        # our pickled state.
        Queue.__init__(self)
        self.maxsize = state[0]
        self.queue = state[1]
        self.unfinished_tasks = state[2]


# prepare for using functools.partial()
def get_fun(fun):
    return fun


class JobQueueManager(SyncManager):
    pass


def clear_queue(q):
    while not q.empty():
        q.get()


def start_worker_in_background(HOST, PORT, AUTHKEY, nprocs, debug_level, limit_nprocs_to_cpucores):
    from minimalcluster import WorkerNode
    worker = WorkerNode(HOST, PORT, AUTHKEY, nprocs, debug_level, limit_nprocs_to_cpucores)
    worker.join_cluster()


class MasterNode:

    def __init__(self, HOST='127.0.0.1', PORT=8888, AUTHKEY=None, chunk_size=50, debug_level=2):
        """
        Method to initiate a master node object.

        Args:
            HOST (str): the hostname or IP address to use
            PORT (int): the port to use
            AUTHKEY (str): The process's authentication key (a string or byte string).
                     If None is given, a random string will be given
            chunk_size (int): The numbers are split into chunks. Each chunk is pushed into the job queue.
                       Here the size of each chunk if specified.
        """

        # Check & process AUTHKEY
        # to [1] ensure compatibility between Py 2 and 3; [2] to allow both string and byte string for AUTHKEY input.
        assert type(AUTHKEY) in [str, bytes] or AUTHKEY is None, \
            "AUTKEY must be either one among string, byte string, and None (a random AUTHKEY will be generated if None is given)."
        if AUTHKEY is not None and type(AUTHKEY) == str:
            AUTHKEY = AUTHKEY.encode()

        self.HOST = HOST
        self.PORT = PORT
        self.AUTHKEY = AUTHKEY if AUTHKEY is not None else ''.join(random.choice(string.ascii_uppercase) for _ in range(6)).encode()
        self.chunk_size = chunk_size
        self.server_status = 'off'
        self.as_worker = False
        self.target_fun = None
        self.master_fqdn = getfqdn()
        self.pid_as_worker_on_master = None

        self.process_as_worker = None
        self.args_to_share_to_workers = None
        self.envir_statements = None

        global DEBUG_LOG
        DEBUG_LOG = debug_level
        self.debug_level = debug_level

    def join_as_worker(self, nprocs=cpu_count(), debug_level=2, limit_nprocs_to_cpucores=True):
        """
        This method helps start the master node as a worker node as well
        """
        if self.as_worker:
            print("[WARNING] This node has already joined the cluster as a worker node.")
        else:
            self.process_as_worker = Process(target=start_worker_in_background, args=(self.HOST, self.PORT, self.AUTHKEY, nprocs, debug_level, limit_nprocs_to_cpucores))
            self.process_as_worker.start()

            # waiting for the master node joining the cluster as a worker
            while self.master_fqdn not in [w[0] for w in self.list_workers()]:
                pass

            self.pid_as_worker_on_master = [w for w in self.list_workers() if w[0] == self.master_fqdn][0][2]
            self.as_worker = True
            print("[INFO] Current node has joined the cluster as a Worker Node (using {} processors; Process ID: {}).".format(cpu_count(), self.process_as_worker.pid))

    def start_master_server(self, if_join_as_worker=True):
        """
        Method to create a manager as the master node.

        Arguments:
        if_join_as_worker: Boolean.
                        If True, the master node will also join the cluster as worker node. It will automatically run in background.
                        If False, users need to explicitly configure if they want the master node to work as worker node too.
                        The default value is True.
        """
        self.job_q = Queue()
        self.result_q = Queue()
        self.error_q = Queue()
        self.get_envir = Queue()
        self.target_function = Queue()
        self.raw_queue_of_worker_list = Queue()
        self.raw_dict_of_job_history = dict()  # a queue to store the history of job assignment

        # Return synchronized proxies for the actual Queue objects.
        # Note that for "callable=", we don't use `lambda` which is commonly used in multiprocessing examples.
        # Instead, we use `partial()` to wrapper one more time.
        # This is to avoid "pickle.PicklingError" on Windows platform. This helps the codes run on both Windows and Linux/Mac OS.
        # Ref: https://stackoverflow.com/questions/25631266/cant-pickle-class-main-jobqueuemanager

        JobQueueManager.register('get_job_q', callable=partial(get_fun, self.job_q))
        JobQueueManager.register('get_result_q', callable=partial(get_fun, self.result_q))
        JobQueueManager.register('get_error_q', callable=partial(get_fun, self.error_q))
        JobQueueManager.register('get_envir', callable=partial(get_fun, self.get_envir))
        JobQueueManager.register('target_function', callable=partial(get_fun, self.target_function))
        JobQueueManager.register('queue_of_worker_list', callable=partial(get_fun, self.raw_queue_of_worker_list))
        JobQueueManager.register('dict_of_job_history', callable=partial(get_fun, self.raw_dict_of_job_history), proxytype=DictProxy)

        self.manager = JobQueueManager(address=(self.HOST, self.PORT), authkey=self.AUTHKEY)
        self.manager.start()
        self.server_status = 'on'
        print('[{}] Master Node started at {}:{} with authkey `{}`.'.format(str(datetime.datetime.now()), self.HOST, self.PORT, self.AUTHKEY.decode()))

        self.shared_job_q = self.manager.get_job_q()
        self.shared_result_q = self.manager.get_result_q()
        self.shared_error_q = self.manager.get_error_q()
        self.share_envir = self.manager.get_envir()
        self.share_target_fun = self.manager.target_function()
        self.queue_of_worker_list = self.manager.queue_of_worker_list()
        self.dict_of_job_history = self.manager.dict_of_job_history()

        if if_join_as_worker:
            self.join_as_worker()

    def stop_as_worker(self):
        """
        Given the master node can also join the cluster as a worker, we
        also need to have a method to stop it as a worker node (which may
        be necessary in some cases). This method serves this purpose.

        Given the worker node will start a separate process for heartbeat purpose.
        We need to shutdown the heartbeat process separately.

        Args:
            -

        Returns:
            None
        """
        try:
            os.kill(self.pid_as_worker_on_master, signal.SIGTERM)
            self.pid_as_worker_on_master = None
            self.process_as_worker.terminate()
        except AttributeError:
            print("[WARNING] The master node has not started as a worker yet.")
        finally:
            self.as_worker = False
            print("[INFO] The master node has stopped working as a worker node.")

    def list_workers(self, approx_max_workers=1000):
        """
        Return a list of connected worker nodes.

        Each element of this list is a tuple:
            (
                hostname of worker node,
                # of available cores,
                pid of heartbeat process on the worker node,
                if the worker node is working on any work load currently (1:Yes, 0:No)
            )

        Args:
            approx_max_workers (int): approximately what are the maximum number of workers connected to the master not ?

        Returns:
            list: connected worker nodes
        """

        # STEP-1: an element will be PUT into the queue "self.queue_of_worker_list"
        # STEP-2: worker nodes will watch on this queue and attach their information into this queue too
        # STEP-3: this function will collect the elements from the queue and return the list of workers node who responded
        print_debug("entered list_workers(...)", level=4)
        self.queue_of_worker_list.put(".")  # trigger worker nodes to contact master node to show their "heartbeat"
        time.sleep(0.3)  # Allow some time for collecting "heartbeat"

        worker_list = []
        while not self.queue_of_worker_list.empty():
            worker_list.append(self.queue_of_worker_list.get())
            if len(worker_list) > approx_max_workers:
                print(f"*** SERVER DEBUG ***, list_workers(...) overflow" +
                      f"\n\t{type(self.queue_of_worker_list)}" +
                      f"\n\t{len(worker_list)} / {approx_max_workers}")
                while not self.queue_of_worker_list.empty():
                    self.queue_of_worker_list.get()
                break

        print_debug("returning from list_workers(...)", level=4)
        return list(set([w for w in worker_list if w != "."]))

    def load_envir(self, source, from_file=True):
        if from_file:
            with open(source, 'r') as f:
                self.envir_statements = "".join(f.readlines())
        else:
            self.envir_statements = source

    def register_target_function(self, fun_name):
        """
        Register the function to be executed for each task/job

        Args:
            fun_name (str): function/method name to be registered

        Returns:
            None
        """
        self.target_fun = fun_name

    def load_args(self, args):
        """
        List/Tuple of arguments which are to be given to the
        registered function for execution.

        Args:
            args (list or tuple): arguments to be used for execution

        Returns:
            None
        """
        self.args_to_share_to_workers = args

    def __check_target_function(self):
        try:
            exec(self.envir_statements)
        except Exception as e:
            print("[ERROR] The environment statements given can't be executed -> {}".format(str(e)))
            raise

        # Reference: https://stackoverflow.com/questions/624926/how-do-i-detect-whether-a-python-variable-is-a-function
        # See answer by 'nh2'
        if self.target_fun in locals() and isinstance(locals()[self.target_fun], collections.Callable):
            return True
        else:
            return False

    def execute(self, approx_max_job_time=None):
        """
        Start the job execution on the cluster

        Args:
            approx_max_job_time (int, float): Approximately how much time(in seconds) would one task/job take for execution

        Returns:
            None
        """
        if (approx_max_job_time is None) or \
                (isinstance(approx_max_job_time, (int, float,)) and approx_max_job_time <= 0) or \
                (not (approx_max_job_time is None) and not (isinstance(approx_max_job_time, (int, float,)))):
            approx_max_job_time = 2 ** 128

        # Ensure the error queue is empty
        clear_queue(self.shared_error_q)

        if self.target_fun is None:
            print("[ERROR] Target function is not registered yet.")
        elif not self.__check_target_function():
            print("[ERROR] The target function registered (`{}`) can't be built "
                  "with the given environment statements.".format(self.target_fun))
        elif len(self.args_to_share_to_workers) != len(set(self.args_to_share_to_workers)):
            print("[ERROR]The arguments to share with worker nodes are not unique. "
                  "Please check the data you passed to MasterNode.load_args().")
        # elif len(self.list_workers()) == 0:
        #     print("[ERROR] No worker node is available. Can't proceed to execute")
        else:
            print("[{}] Assigning jobs to worker nodes.".format(str(datetime.datetime.now())))

            self.share_envir.put(self.envir_statements)

            self.share_target_fun.put(self.target_fun)

            # The numbers are split into chunks. Each chunk is pushed into the job queue
            local_total_job_count = 0
            # Set containing job_id of all jobs completed
            local_set_job_id_done = set()
            # Time at which last job was completed
            local_last_status_change = float('inf')

            for i in range(0, len(self.args_to_share_to_workers), self.chunk_size):
                self.shared_job_q.put((i, self.args_to_share_to_workers[i:(i + self.chunk_size)]))
                local_total_job_count += 1
                print_debug(f"LEN [{local_total_job_count}] = "
                            f"{len(self.args_to_share_to_workers[i:(i + self.chunk_size)])}", level=5)
            print_debug(f"TOTAL len = {local_total_job_count}", level=3)

            # Wait until all results are ready in shared_result_q
            num_results = 0
            dict_result = {}
            list_job_id_done = []
            while num_results < len(self.args_to_share_to_workers):
                current_workers_list = self.list_workers()
                if len(current_workers_list) == 0:
                    print("[{}][Warning] No valid worker node at this moment. You can wait for "
                          "workers to join, or CTRL+C to cancel.".format(str(datetime.datetime.now())))
                    time.sleep(1)
                    continue
                sum_workers_status = sum([w[3] for w in current_workers_list])
                print_debug(level=3)
                print_debug(f"time since last change = "
                            f"{'∞' if local_last_status_change==float('inf') else int(time.time() - local_last_status_change)}"
                            f" / {approx_max_job_time}", level=3)
                print_debug(f"val self.shared_job_q.empty() = {self.shared_job_q.empty()}", level=3)
                print_debug(f"val sum_workers_status = {sum_workers_status:4} / {len(current_workers_list):4}", level=3)
                print_debug(f"val results computed   = {num_results} / {len(self.args_to_share_to_workers)}", level=3)
                print_debug(f"val jobs completed     = {len(local_set_job_id_done)} / {local_total_job_count}", level=3)
                if self.shared_job_q.empty() and \
                        (sum_workers_status == 0 or ((time.time() - local_last_status_change) > approx_max_job_time)):
                    '''
                    After all jobs are assigned and all worker nodes have finished their works,
                    check if the nodes who have un-finished jobs are sill alive.
                    if not, re-collect these jobs and put them in to the job queue
                    '''
                    if (time.time() - local_last_status_change) > approx_max_job_time:
                        local_last_status_change = time.time()
                    print_debug(f"val self.shared_result_q.empty() = {self.shared_result_q.empty()}", level=3)
                    print_debug(f"val len(list_job_id_done) = {len(list_job_id_done):}", level=3)
                    while not self.shared_result_q.empty():
                        local_last_status_change = time.time()
                        try:
                            job_id_done, outdict = self.shared_result_q.get(False)
                            if job_id_done not in local_set_job_id_done:
                                print_debug(
                                    f"******** job_id, len(outdict) = {job_id_done:6}, {len(outdict):6}, "
                                    f"{len(outdict) == len(self.args_to_share_to_workers[job_id_done: job_id_done + self.chunk_size])}",
                                    level=3
                                )
                                dict_result.update(outdict)
                                list_job_id_done.append(job_id_done)
                                num_results += len(outdict)
                                local_set_job_id_done.add(job_id_done)
                        except:
                            pass

                    [self.dict_of_job_history.pop(k, None) for k in list_job_id_done]
                    list_job_id_done = []

                    approx_max_job_time += 5  # NEW: 20191206
                    for job_id in self.dict_of_job_history.keys():
                        print("Putting {} back to the job queue".format(job_id))
                        self.shared_job_q.put((job_id, self.args_to_share_to_workers[job_id:(job_id + self.chunk_size)]))

                if not self.shared_error_q.empty():
                    print("[ERROR] Running error occurred in remote worker node:")
                    while not self.shared_error_q.empty():
                        error_message = self.shared_error_q.get()
                        if not error_message.startswith(prefix="#Error#"):
                            print(error_message)
                            continue
                        try:
                            error_host_name, error_job_id, error_job_len = error_message[7:].split("#")
                            error_job_id, error_job_len = int(error_job_len), int(error_job_len)
                        except Exception as e:
                            error_host_name, error_job_id, error_job_len = "Unknown", -1, self.chunk_size
                            print_debug(str(e), level=2)
                        list_job_id_done.append(error_job_id)
                        num_results += error_job_len
                        local_set_job_id_done.add(error_job_id)
                        self.dict_of_job_history.pop(error_job_id, None)
                        print("[Error] hostname={}, job_id={}, job_len={}".format(
                            error_host_name, error_job_id, error_job_len
                        ))

                    # NOTE: the following lines are commented as execution error in one
                    # remote worker node shall not affect execution of the whole cluster.

                    # clear_queue(self.shared_job_q)
                    # clear_queue(self.shared_result_q)
                    # clear_queue(self.share_envir)
                    # clear_queue(self.share_target_fun)
                    # clear_queue(self.shared_error_q)
                    # self.dict_of_job_history.clear()
                    # return None

                # job_id_done is the unique id of the jobs that have been done and returned to the master node.
                while not self.shared_result_q.empty():
                    local_last_status_change = time.time()
                    try:
                        job_id_done, outdict = self.shared_result_q.get(False)
                        if job_id_done not in local_set_job_id_done:  # NEW 11/19
                            print_debug(
                                f"******** job_id, len(outdict) = {job_id_done}, {len(outdict)}, "
                                f"{len(outdict) == len(self.args_to_share_to_workers[job_id_done: job_id_done + self.chunk_size])}",
                                level=3
                            )
                            dict_result.update(outdict)
                            list_job_id_done.append(job_id_done)
                            num_results += len(outdict)
                            local_set_job_id_done.add(job_id_done)
                    except:
                        pass

                time.sleep(0.92)

            print_debug(f"DEBUG: master lib = {len(dict_result)}, {num_results}, {len(self.args_to_share_to_workers)}", level=3)
            print("[{}] Aggregating on Master node...".format(str(datetime.datetime.now())))

            # After the execution is done, empty all the args & task function queues
            # to prepare for the next execution
            clear_queue(self.shared_job_q)
            clear_queue(self.shared_result_q)
            clear_queue(self.share_envir)
            clear_queue(self.share_target_fun)
            self.dict_of_job_history.clear()

            return dict_result

    def shutdown(self):
        if self.as_worker:
            self.stop_as_worker()

        if self.server_status == 'on':
            self.manager.shutdown()
            self.server_status = "off"
            print("[INFO] The master node is shut down.")
        else:
            print("[WARNING] The master node is not started yet or already shut down.")
