"""
Test script for raydp-submit --py-files functionality.
This script mimics raydp-submit.py but tests that --py-files works correctly.
"""
from os.path import dirname, abspath, join
import sys
import json
import subprocess
import shlex
import ray

def main():
    print("Starting raydp-submit --py-files test...")

    # Initialize Ray and get cluster info
    ray.init(address="auto")
    node = ray.worker.global_worker.node
    options = {}
    options["ray"] = {}
    options["ray"]["run-mode"] = "CLUSTER"
    options["ray"]["node-ip"] = node.node_ip_address
    options["ray"]["address"] = node.address
    options["ray"]["session-dir"] = node.get_session_dir_path()

    ray.shutdown()

    # Write Ray configuration
    examples_dir = dirname(abspath(__file__))
    conf_path = join(examples_dir, "ray.conf")
    with open(conf_path, "w") as f:
        json.dump(options, f)

    # Build raydp-submit command
    command = ["bin/raydp-submit", "--ray-conf", conf_path]
    command += ["--conf", "spark.executor.cores=1"]
    command += ["--conf", "spark.executor.instances=1"]
    command += ["--conf", "spark.executor.memory=500m"]

    # Add --py-files with test_pyfile.py
    test_pyfile_path = join(examples_dir, "test_pyfile.py")
    command += ["--py-files", test_pyfile_path]

    # Add the main script
    main_script_path = join(examples_dir, "test_pyfiles_main.py")
    command.append(main_script_path)

    # Execute the command
    print("\nExecuting command:")
    cmd_str = " ".join(shlex.quote(arg) for arg in command)
    print(cmd_str)
    print("\n" + "=" * 60)

    result = subprocess.run(cmd_str, check=True, shell=True)

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
