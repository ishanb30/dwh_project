"""
Copying Source CSVs to Docker Container

Loops through the source CSVs and copies them to
the Docker container. This is needed so that SQL
Server's BULK INSERT can access the files on
Linux.
"""

import subprocess
from paths import SOURCE_CSV_DIR

class SourceToDockerFailed(Exception):
    def __init__(self, failed_copy):
        self.failed_copy = failed_copy
        super().__init__(failed_copy)

def run_source_to_docker():
    try:
        files = list(SOURCE_CSV_DIR.rglob("*csv"))

        for file in files:
            subprocess.run(
                ["docker", "cp", file, f"mssql_server:/var/opt/mssql/data/{file.name.lower()}"]
                , check=True
            )
    except subprocess.CalledProcessError as e:
        raise SourceToDockerFailed(f"{e}: Failed to copy {file.name}")

if __name__ == "__main__":
    run_source_to_docker()
