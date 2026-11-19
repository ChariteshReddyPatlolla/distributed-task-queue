import sys
from pathlib import Path

_dir = str(Path(__file__).resolve().parent)
if _dir not in sys.path:
    sys.path.insert(0, _dir)

from . import task_queue_pb2, task_queue_pb2_grpc

__all__ = ['task_queue_pb2', 'task_queue_pb2_grpc']
