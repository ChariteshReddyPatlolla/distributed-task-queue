import re
import sys
from pathlib import Path
from grpc_tools import protoc


def fix_grpc_import(grpc_file: Path, package_prefix: str = ""):
    if not grpc_file.exists():
        return
    content = grpc_file.read_text(encoding="utf-8")
    # Replace bare 'import task_queue_pb2 as task__queue__pb2'
    # with try-except for both package and top-level imports
    replacement = (
        "try:\n"
        f"    from {package_prefix} import task_queue_pb2 as task__queue__pb2\n"
        "except (ImportError, ValueError):\n"
        "    try:\n"
        "        from . import task_queue_pb2 as task__queue__pb2\n"
        "    except (ImportError, ValueError):\n"
        "        import task_queue_pb2 as task__queue__pb2"
    ) if package_prefix else (
        "try:\n"
        "    from . import task_queue_pb2 as task__queue__pb2\n"
        "except (ImportError, ValueError):\n"
        "    import task_queue_pb2 as task__queue__pb2"
    )
    
    new_content = re.sub(
        r"import task_queue_pb2 as task__queue__pb2",
        replacement,
        content
    )
    grpc_file.write_text(new_content, encoding="utf-8")


def main():
    root_dir = Path(__file__).resolve().parent.parent
    proto_dir = root_dir / "proto"
    common_proto_dir = root_dir / "services" / "common" / "proto"
    common_proto_dir.mkdir(parents=True, exist_ok=True)

    proto_file = proto_dir / "task_queue.proto"

    # Generate stubs into services/common/proto
    res1 = protoc.main([
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={common_proto_dir}",
        f"--grpc_python_out={common_proto_dir}",
        str(proto_file),
    ])

    # Also generate stubs into proto/
    res2 = protoc.main([
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={proto_dir}",
        f"--grpc_python_out={proto_dir}",
        str(proto_file),
    ])

    if res1 != 0 or res2 != 0:
        print(f"Error generating stubs: {res1}, {res2}", file=sys.stderr)
        sys.exit(1)

    # Fix relative imports in grpc files
    fix_grpc_import(common_proto_dir / "task_queue_pb2_grpc.py", "services.common.proto")
    fix_grpc_import(proto_dir / "task_queue_pb2_grpc.py", "proto")

    # Write __init__.py files that expose the stubs
    init_content_common = (
        "import sys\n"
        "from pathlib import Path\n\n"
        "_dir = str(Path(__file__).resolve().parent)\n"
        "if _dir not in sys.path:\n"
        "    sys.path.insert(0, _dir)\n\n"
        "from . import task_queue_pb2, task_queue_pb2_grpc\n\n"
        "__all__ = ['task_queue_pb2', 'task_queue_pb2_grpc']\n"
    )
    (common_proto_dir / "__init__.py").write_text(init_content_common, encoding="utf-8")

    init_content_proto = (
        "import sys\n"
        "from pathlib import Path\n\n"
        "_dir = str(Path(__file__).resolve().parent)\n"
        "if _dir not in sys.path:\n"
        "    sys.path.insert(0, _dir)\n\n"
        "from . import task_queue_pb2, task_queue_pb2_grpc\n\n"
        "__all__ = ['task_queue_pb2', 'task_queue_pb2_grpc']\n"
    )
    (proto_dir / "__init__.py").write_text(init_content_proto, encoding="utf-8")

    print("Protobuf and gRPC stubs generated and configured successfully.")


if __name__ == "__main__":
    main()
