"""`python -m pymqttd ...` runs the executable with the same arguments."""

import os
import sys

from pymqttd import find_executable


def main() -> None:
    exe = find_executable()
    if sys.platform == "win32":
        import subprocess

        sys.exit(subprocess.call([exe, *sys.argv[1:]]))
    os.execv(exe, [exe, *sys.argv[1:]])


if __name__ == "__main__":
    main()
