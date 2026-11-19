# Database Instances

Database instance files for **IMDB**, **TPC-H**, and **TPC-DS** are available in
the [DiagDBench dataset](https://huggingface.co/datasets/zhaoxinxin798/DiagDBench)
on Hugging Face 🤗.

This directory contains only this README in the GitHub checkout. Download the
database files before initializing the benchmark databases.

## Download

Run from the repository root:

```bash
python -m pip install -U huggingface_hub
hf download zhaoxinxin798/DiagDBench --repo-type dataset \
  --include "Database_Instances/**" --local-dir .
```

The command creates the database subdirectories shown below without replacing
this README.

## Layout After Download

```text
Database_Instances/
├── README.md
├── postgresql_instances/
│   ├── imdb.dump
│   ├── tpch_1.dump
│   └── tpcds_1.dump
└── mysql_instances/
    ├── imdb.load/
    ├── tpch_1.load/
    └── tpcds_1.load/
```

For MySQL, each `.load/` package contains `schema.sql`, `post_load.sql`,
`manifest.tsv`, and table data files in `data/*.tsv`. Download the complete
package rather than individual TSV files.

For PostgreSQL, each `.dump` file is a complete SQL dump.
