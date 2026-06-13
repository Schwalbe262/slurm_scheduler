from __future__ import annotations

import argparse
import getpass
import os

import paramiko


def main() -> None:
    parser = argparse.ArgumentParser(description="Check SFTP connectivity and list remote paths.")
    parser.add_argument("--host", default=os.environ.get("SFTP_HOST", ""))
    parser.add_argument("--port", type=int, default=int(os.environ.get("SFTP_PORT", "22")))
    parser.add_argument("--user", default=os.environ.get("SFTP_USER", ""))
    parser.add_argument("--path", action="append", default=[])
    args = parser.parse_args()
    if not args.host or not args.user:
        raise SystemExit("provide --host/--user or set SFTP_HOST/SFTP_USER")
    paths = args.path or os.environ.get("SFTP_PATHS", "/").split(",")

    password = os.environ.get("SFTP_PASSWORD")
    if password is None:
        password = getpass.getpass("SFTP password: ")

    transport = paramiko.Transport((args.host, args.port))
    try:
        transport.connect(username=args.user, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        try:
            for remote_path in paths:
                print(f"{remote_path}:")
                for item in sftp.listdir_attr(remote_path)[:20]:
                    marker = "d" if str(item.longname).startswith("d") else "-"
                    print(f"  {marker} {item.filename}")
        finally:
            sftp.close()
    finally:
        transport.close()


if __name__ == "__main__":
    main()
