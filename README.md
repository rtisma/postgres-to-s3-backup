# Postgres DB to S3 Backup
---

# Description
Does a pg_dump on a database (configured via an input ini file), gzips it, and uploads it to a specific bucket in s3

# Installation

```bash
pip3 install -r requirements.txt
```

# Usage

```bash
python postgre_db_s3_backup.py  ./example.ini

```



