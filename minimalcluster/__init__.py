__all__ = ['MasterNode', "WorkerNode", 'set_debug_level', 'get_debug_level']

import sys

if sys.version_info.major == 3:
    from .master_node import MasterNode
    from .worker_node import WorkerNode, set_debug_level, get_debug_level
else:
    from master_node import MasterNode
    from worker_node import WorkerNode, set_debug_level, get_debug_level
