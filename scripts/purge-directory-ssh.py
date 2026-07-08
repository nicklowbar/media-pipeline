#!/usr/bin/env python3
"""Copy purge-directory.sh to the remote host and run it there.

Uses scp to copy the script, then a single ssh command to run it — no shell
quoting of the script content, no nested escaping, no shell interpretation
of paths.

Usage:
    purge-directory-ssh.py <ssh-host> [--db-path PATH] [-v] <staging-path>
"""

import argparse
import subprocess
import sys
import os


def main():
    parser = argparse.ArgumentParser(
        description="Copy purge-directory.sh to remote host and run it."
    )
    parser.add_argument("ssh_host", help="SSH host to connect to")
    parser.add_argument(
        "--db-path",
        default="/opt/media-pipeline/data/pipeline.db",
        help="Path to pipeline.db on remote host",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose output"
    )
    parser.add_argument("staging_path", help="Staging path to purge")
    args = parser.parse_args()

    ssh_host = args.ssh_host
    db_path = args.db_path
    staging_path = args.staging_path
    verbose = args.verbose

    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "purge-directory.sh")
    if not os.path.exists(script_path):
        raise FileNotFoundError(f"purge-directory.sh not found at {script_path}")

    remote_tmp = f"/tmp/purge-directory-{os.getpid()}"

    print(f"copying script to {ssh_host}:{remote_tmp} ...")
    subprocess.run(["scp", script_path, f"{ssh_host}:{remote_tmp}"], check=True)

    print(f"running on {ssh_host} ...")
    if verbose:
        print(f"  staging_path = {staging_path}", file=sys.stderr)

    # Build the remote bash -c command. Apostrophes in paths are escaped as
    # '\'' (end quote, literal \, start new quote) which is safe inside a
    # single-quoted bash string. The whole remote_cmd is passed as a single
    # string to subprocess — no list argv splitting.
    safe_staging = staging_path.replace("'", "'\\''")
    safe_db = db_path.replace("'", "'\\''")
    verbose_flag = " -v" if verbose else ""
    remote_cmd = (
        f"chmod +x '{remote_tmp}' && "
        f"'{remote_tmp}' --db-path '{safe_db}' --running 0{verbose_flag} '{safe_staging}' && "
        f"rm -f '{remote_tmp}'"
    )

    result = subprocess.run(
        ["ssh", ssh_host, remote_cmd],
        text=True,
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    main()
