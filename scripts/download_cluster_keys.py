from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path

import paramiko


def main() -> None:
    parser = argparse.ArgumentParser(description="Download cluster PEM keys from the configured SFTP share.")
    parser.add_argument("--host", default=os.environ.get("SFTP_HOST", ""))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SFTP_PORT", "22")))
    parser.add_argument("--user", default=os.environ.get("SFTP_USER", ""))
    parser.add_argument("--remote-dir", default=os.environ.get("SFTP_KEY_DIR", ""))
    parser.add_argument("--output-dir", default="secrets/cluster")
    parser.add_argument("--key", action="append", default=[])
    args = parser.parse_args()
    if not args.host or not args.user or not args.remote_dir:
        raise SystemExit("provide --host/--user/--remote-dir or set SFTP_HOST/SFTP_USER/SFTP_KEY_DIR")
    if not args.key:
        raise SystemExit("provide one or more --key values")

    password = os.environ.get("SFTP_PASSWORD")
    if password is None:
        password = getpass.getpass("SFTP password: ")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir.chmod(0o700)

    key_names = args.key
    transport = paramiko.Transport((args.host, args.port))
    try:
        transport.connect(username=args.user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            for key_name in key_names:
                remote_path = f"{args.remote_dir.rstrip('/')}/{key_name}"
                local_path = output_dir / key_name
                sftp.get(remote_path, str(local_path))
                local_path.chmod(0o600)
                print(f"downloaded {remote_path} -> {local_path}")
        finally:
            sftp.close()
    finally:
        transport.close()


if __name__ == "__main__":
    main()
