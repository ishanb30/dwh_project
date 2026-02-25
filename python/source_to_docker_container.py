"""
Copying Source CSVs to Docker Container

Loops through the source CSVs and copies them to
the Docker container. This is needed so that SQL
Server's BULK INSERT can access the files on
Linux.
"""

import subprocess
from config import SOURCE_CSV_DIR

def run_source_to_docker():
    try:
        files = list(SOURCE_CSV_DIR.rglob("*csv"))

        for file in files:
            subprocess.run(
                ["docker", "cp", {file}, f"mssql_server:/var/opt/mssql/data/{file.name.lower()}"]
                , check=True
            )
    except subprocess.CalledProcessError as e:
        raise Exception(f"{e}: Failed to copy {file.name}")

#will be part of master orchestrator
if __name__ == "__main__":
    run_source_to_docker()
