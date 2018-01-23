# Postgres DB to S3 Backup
---

# Description
Does a pg_dump on a database (configured via an input ini file), gzips it, and uploads it to a specific bucket in AWS S3. If the bucket does not exist, it is automatically created. All backups have a user defined prefix, and ends in the format `_YYYYMMDD_HHmmSS_ZZZ.sql.gz`

# Installation

```bash
pip3 install -r requirements.txt
```

# Usage

```bash
python3 postgre_db_s3_backup.py  ./example.ini

```



