from configparser import ConfigParser
import datetime
import logging.config
import os
import re
import sys
from threading import Thread
import connectionchecks

import urllib3
import yaml

from ambari_cluster_extractor import AmbariApiExtractor
from hive_metastore_extractor import HiveMetastoreExtractor
from metrics_discovery import MetricsExtractor

root_path = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(root_path, 'conf', 'log-config.yaml'), 'r') as stream:
    config = yaml.load(stream, Loader=yaml.FullLoader)
    log_path = config['handlers']['file']['filename']
    config['handlers']['file']['filename'] = os.path.join(root_path, log_path)
logging.config.dictConfig(config)
log = logging.getLogger('main')

log.debug("root path =>" + root_path)
log.debug("log path =>" + log_path)

def get_config_params(config_file):
  try:
    with open(config_file) as f:
      try:
        parser = ConfigParser()
        parser.read_file(f)
      except ConfigParser.Error as  err:
        log.error('Could not parse: {} '.format(err))
        return False
  except IOError as e:
    log.error("Unable to access %s. Error %s \nExiting" % (config_file, e))
    sys.exit(1)

  ambari_server_host = parser.get('ambari_config', 'ambari_server_host')
  ambari_server_port = parser.get('ambari_config', 'ambari_server_port')
  ambari_user = parser.get('ambari_config', 'ambari_user')
  ambari_pass = parser.get('ambari_config', 'ambari_pass')
  ambari_server_timeout = parser.get('ambari_config', 'ambari_server_timeout')
  ambari_http_protocol = parser.get('ambari_config', 'ambari_http_protocol')
  cluster_name = parser.get('ambari_config', 'cluster_name')
  output_dir = parser.get('ambari_config', 'output_dir')

  hive_metastore_type = parser.get('hive_config', 'hive_metastore_type')
  hive_metastore_server = parser.get('hive_config', 'hive_metastore_server')
  hive_metastore_server_port = parser.get('hive_config', 'hive_metastore_server_port')
  hive_metastore_database_name = parser.get('hive_config', 'hive_metastore_database_name')
  hive_metastore_database_user = parser.get('hive_config', 'hive_metastore_database_user')
  hive_metastore_database_password = parser.get('hive_config', 'hive_metastore_database_password')


  if not ambari_server_port.isdigit():
    log.error("Invalid port specified for Ambari Server. Exiting")
    sys.exit(1)
  if not connectionchecks.is_valid_hostname(ambari_server_host):
    log.error("Invalid hostname provided for Ambari Server. Exiting")
    sys.exit(1)
  if not ambari_server_timeout.isdigit():
    log.error("Invalid timeout value specified for Ambari Server. Using default of 30 seconds")
    ambari_server_timeout = 30

  # Prepare dictionary object with config variables populated for both anmabri and ranger.
  config_dict = {}
  config_dict["ambari_server_host"] = ambari_server_host
  config_dict["ambari_server_port"] = ambari_server_port
  config_dict["ambari_server_timeout"] = ambari_server_timeout
  config_dict["output_dir"] = output_dir
  config_dict["hive_metastore_type"] = hive_metastore_type
  config_dict["hive_metastore_server"] = hive_metastore_server
  config_dict["hive_metastore_server_port"] = hive_metastore_server_port
  config_dict["hive_metastore_database_name"] = hive_metastore_database_name
  config_dict["hive_metastore_database_password"] = hive_metastore_database_password
  config_dict["hive_metastore_database_user"] = hive_metastore_database_user
  config_dict["ambari_http_protocol"] = ambari_http_protocol



  if re.match(r'^[A-Za-z0-9_]+$', cluster_name):
    config_dict["cluster_name"] = cluster_name
  else:
    log.error("Invalid Cluster name provided. Cluster name should have only alphanumeric characters and underscore. Exiting")
    return False

  if re.match(r'^[a-zA-Z0-9_.-]+$', ambari_user):
    config_dict["ambari_user"] = ambari_user
  else:
    log.error("Invalid Username provided. Exiting")
    return False

  config_dict["ambari_pass"] = ambari_pass

  return config_dict


if __name__ == '__main__':

    start_timestamp = datetime.datetime.utcnow()
    log.debug("Current timestamp to use in metric collection: %s", start_timestamp.replace(microsecond=0).isoformat())

    threads = []

    #ambari_conf = {}
    ambari_conf = get_config_params(os.path.join(root_path, 'conf', 'config.ini'))

    # TODO: Add options for module choices
    module='all'

    if module == 'all' or module == 'ambari_api':
        ambari_api_extractor = AmbariApiExtractor(ambari_conf)
        threads.append(Thread(target=ambari_api_extractor.collect_ambari_api_diagnostic, name="ambari_api_thread"))

    if module == 'all' or module == 'hive_metastore':
        hive_ms_extractor = HiveMetastoreExtractor(ambari_conf)
        threads.append(Thread(target=hive_ms_extractor.collect_metastore_info, name="hive_ms_thread"))

    # if module == 'all' or module == 'extract_metrics':
    #     metrics_extractor = MetricsExtractor(ambari_conf)
    #     threads.append(Thread(target=metrics_extractor.collect_metrics, name="metrics_collector_thread"))

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
