import csv
import logging.config
import os
from pathlib import Path

import jaydebeapi

root_path = os.path.dirname(os.path.realpath(__file__))
log = logging.getLogger('main')
log.setLevel(logging.INFO)

psql = {
    'driver_class': 'org.postgresql.Driver',
    'path': os.path.join(root_path, 'drivers/postgresql-connector-java.jar'),
    'query': 'select distinct "NAME" as "DB_NAME", "TBL_NAME","PART_NAME" IS NOT NULL as "IS_PARTITIONED","PKEY_NAME", "TBL_TYPE", "OWNER","DB_LOCATION_URI" from "TBLS" join "DBS" on "DBS"."DB_ID"="TBLS"."DB_ID" left join "PARTITIONS" on "TBLS"."TBL_ID"="PARTITIONS"."TBL_ID" left join "PARTITION_KEYS" on "PARTITION_KEYS"."TBL_ID"="TBLS"."TBL_ID";',
    'jdbc_format': 'jdbc:{db_type}://{db_host}:{db_port}/{db_name}'
}

mysql = {
    'driver_class': 'com.mysql.jdbc.Driver',
    'path': os.path.join(root_path, 'drivers/mysql-connector-java.jar'),
    'query': 'select name as datbase_name , tbl_name, PART_NAME IS NOT NULL as is_partitioned, PKEY_NAME, count(PKEY_NAME) as partition_count, tbl_type, db_location_uri from TBLS join DBS on TBLS.db_id=DBS.db_id left join PARTITIONS on TBLS.tbl_id=PARTITIONS.tbl_id left join PARTITION_KEYS on PARTITION_KEYS.tbl_id=TBLS.tbl_id group by name, tbl_name;',
    'jdbc_format': 'jdbc:{db_type}://{db_host}:{db_port}/{db_name}'
}

oracle = {
    'driver_class': 'oracle.jdbc.driver.OracleDriver',
    'path': os.path.join(root_path, 'drivers/oracle-connector-java.jar'),
    'query': "select distinct name as database_name , tbl_name, pkey_name, case nvl(pkey_name,'false') when 'false' then 'f' else 't' end as is_partitioned, tbl_type, db_location_uri from TBLS join DBS on TBLS.db_id=DBS.db_id left join PARTITIONS on TBLS.tbl_id=PARTITIONS.tbl_id left join PARTITION_KEYS on PARTITION_KEYS.tbl_id=TBLS.tbl_id",
    'jdbc_format': 'jdbc:{db_type}:thin:@{db_host}:{db_port}/{db_name}'
}

db_constants = {
    'postgresql': psql,
    'mysql': mysql,
    'mariadb': mysql,
    'oracle': oracle
}


def create_directory(dir_path):
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)


def write_csv(columns, rows, output):
    csv_file = open(output, mode='w')
    writer = csv.writer(csv_file, delimiter=',', lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)
    log.debug("CSV write finished, results at: {output}")


class HiveMetastoreExtractor:
    def __init__(self, ambari_conf):
        self.output_dir = ambari_conf['output_dir']
        self.hive_metastore_type = ambari_conf['hive_metastore_type']
        self.hive_metastore_server = ambari_conf['hive_metastore_server']
        self.hive_metastore_server_port = ambari_conf['hive_metastore_server_port']
        self.hive_metastore_database_name = ambari_conf['hive_metastore_database_name']
        self.hive_metastore_database_user = ambari_conf['hive_metastore_database_user']
        self.hive_metastore_database_password = ambari_conf['hive_metastore_database_password']

    def collect_metastore_info(self):
        db_type = self.hive_metastore_type
        db_host = self.hive_metastore_server
        db_port = self.hive_metastore_server_port
        db_name = self.hive_metastore_database_name
        db_user = self.hive_metastore_database_user
        db_password = self.hive_metastore_database_password
        db_constant = db_constants.get(db_type)
        if not db_constant:
            log.error("Unsupported database type: {db_type}")
            exit(-1)
        log.debug("Connecting to {db_name} database on {db_host}")
        conn = jaydebeapi.connect(
            db_constant['driver_class'],
            db_constant['jdbc_format'].format(db_type=db_type, db_host=db_host, db_port=db_port, db_name=db_name),
            [db_user, db_password],
            db_constant['path'])
        curs = conn.cursor()
        log.debug("Executing query: {db_constant['query']}")
        curs.execute(db_constant['query'])
        columns = [column_description[0] for column_description in curs.description]
        rows = curs.fetchall()
        write_csv(columns, rows, os.path.join(self.output_dir, "hive_ms.csv"))
        log.debug("Hive Metastore collection finished.")