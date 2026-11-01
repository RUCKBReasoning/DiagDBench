# Database seed files

This directory contains PostgreSQL dumps and MySQL loading files for IMDB,
TPC-H, and TPC-DS. Dumps, TSV files, and dump parts are stored with Git LFS.
SQL schema and post-load scripts are ordinary Git files.

The PostgreSQL `imdb.dump` is 4,091,050,346 bytes, larger than the smallest
GitHub LFS per-file limit. It is distributed as four ordered parts:

| Part | Bytes |
|---|---:|
| `postgresql_instances/imdb.dump.part000` | 1,073,741,824 |
| `postgresql_instances/imdb.dump.part001` | 1,073,741,824 |
| `postgresql_instances/imdb.dump.part002` | 1,073,741,824 |
| `postgresql_instances/imdb.dump.part003` | 869,824,874 |

`postgresql_instances/imdb.dump.parts.json` records each part's size and SHA-256,
plus the size and SHA-256 of the complete dump. The complete `imdb.dump` is
ignored by Git; keep it locally for the existing database initialization code.

## Download and restore

With Git LFS installed, run these commands from the repository root:

```bash
git lfs pull --include="DiagDBench/*.zip,Database_Instances/**"

# Read-only verification of all IMDB parts and any existing complete dump.
python3 -B Database_Instances/restore_imdb.py --check

# Reconstruct Database_Instances/postgresql_instances/imdb.dump.
python3 -B Database_Instances/restore_imdb.py
```

The restoration command requires enough free disk space for another 4.1 GB.
It verifies the part order, sizes, individual hashes, and combined hash before
publishing the restored file. Missing parts, Git LFS pointer stubs, and corrupt
parts fail validation. A matching existing dump is reused; a different existing
dump is never overwritten. Interrupted restores cannot publish a partial dump;
an abrupt process termination may leave an ignored `imdb.dump.restore-*` file.
Once the command is no longer running, that temporary file can be removed.

Restore the dump before running the normal database initialization commands.
The loader paths remain unchanged. TPC-H and TPC-DS dumps and the MySQL TSV
files are below the publication size ceiling and need no reassembly.

## Publishing updates

The sibling `push_to_github.sh` release script includes `DiagDBench/Train.zip`,
`DiagDBench/Eval.zip`, database loading files, and the IMDB parts. It rejects LFS
files larger than 2,000,000,000 bytes and verifies the IMDB manifest before
staging or uploading. Extracted dataset directories and `Data/` remain local.

If the original IMDB dump changes, regenerate its parts and manifest together.
An existing original dump that no longer matches the manifest causes the
release check to fail. Do not publish a mixture of parts from different dumps.
