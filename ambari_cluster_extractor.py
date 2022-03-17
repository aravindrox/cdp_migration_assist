import json
import logging
import os
import base64
import urllib.request
from pathlib import Path
#from configmapping import get_component_config, filter_parameters
from connectionchecks import test_socket

log = logging.getLogger('main')

def dump_json(path, api_response):
    f = open(path, "w")
    f.write(json.dumps(api_response))
    log.debug(f"Api response stored in: {path}")


def create_directory(dir_path):
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)


class AmbariApiExtractor:
    def __init__(self, ambari_conf):
        self.ambari_server_host = ambari_conf['ambari_server_host']
        self.ambari_server_port = ambari_conf['ambari_server_port']
        self.api_output_dir = ambari_conf['output_dir']
        self.cluster_name = ambari_conf['cluster_name']
        self.ambari_user = ambari_conf['ambari_user']
        self.ambari_pass = ambari_conf['ambari_pass']
        self.ambari_server_timeout = ambari_conf['ambari_server_timeout']
        self.url_suffix=""
        self.host_list=[]
        self.service_list=[]

    def collect_ambari_api_diagnostic(self):
        create_directory(self.api_output_dir)
        self.collect_hosts()
        self.collect_hosts_cpu_mem()
        self.collect_service_info()
        # self.collect_kerberos_info()

    # Print response from the Ambari server
    def send_ambari_request(self,url_suffix):

        # Test socket connectivity to Ambari server port
        test_socket(
            str(self.ambari_server_host),
            int(self.ambari_server_port),
            "Ambari server"
        )

        # Construct URL request for metrics data
        base_url = "http://{}:{}/api/v1/clusters/{}".format(
            str(self.ambari_server_host),
            int(self.ambari_server_port),
            str(self.cluster_name)
        )

        url = "{}{}".format(base_url, url_suffix)
        log.debug("Connecting to URL " + url)
        auth_string = "{}:{}".format(self.ambari_user,self.ambari_pass)
        auth_encoded = 'Basic {}'.format(
            base64.urlsafe_b64encode(
                auth_string.encode('UTF-8')
            ).decode('ascii')
        )
        req = urllib.request.Request(url)
        req.add_header('Authorization', auth_encoded)

        httpHandler = urllib.request.HTTPHandler()
        # httpHandler.set_http_debuglevel(1)
        opener = urllib.request.build_opener(httpHandler)

        try:
            response = opener.open(req, timeout=int(self.ambari_server_timeout))
            return json.load(response)
        except (urllib.request.URLError, urllib.request.HTTPError) as e:
            log.error('Requested URL not found. Error:{}'.format(e))

    def collect_hosts(self):
        log.debug("Read host list.")
        hosts_list_api_response = self.send_ambari_request("/hosts")
        hosts_dict = {}
        hosts_dict[self.cluster_name]=[]
        for host_name in hosts_list_api_response['items']:
            hosts_dict[self.cluster_name].append(host_name['Hosts']['host_name'])
        dump_json(os.path.join(self.api_output_dir, "read_hosts.json"), hosts_dict)
        self.host_list = hosts_dict[self.cluster_name]

    def collect_service_info(self):
        log.debug("Read service information.")
        yarn_info_api_response = self.send_ambari_request("/services/YARN/components/NODEMANAGER")
        yarn_info = {}
        #yarn_info['AvailableVCores'] = yarn_info_api_response['metrics']['yarn']['Queue']['root']['AvailableVCores']
        #yarn_info['AvailableMB'] = yarn_info_api_response['metrics']['yarn']['Queue']['root']['AvailableMB']
        yarn_queue_info_api_response_raw = self.send_ambari_request("/configurations/service_config_versions?service_name=YARN")
        yarn_queue_info_api_response =  yarn_queue_info_api_response_raw['items'][len(yarn_queue_info_api_response_raw['items'])-1]
        capacity_scheduler = {}
        for x in yarn_queue_info_api_response.items():
            if x[0] == "configurations":
                for y in range(len(x[1])):
                    for z in (x[1][y]).items():
                        if z[0] == 'type' and  z[1] == 'capacity-scheduler':
                            capacity_scheduler = x[1][y]['properties']
        dump_json(os.path.join(self.api_output_dir, "yarn_info.json"), yarn_info)
        dump_json(os.path.join(self.api_output_dir, "capacity_scheduler.json"), capacity_scheduler)

    def collect_hosts_cpu_mem(self):
        log.debug("Read host information.")
        total_mem = {}
        total_vcpu = {}
        total_pcpu = {}
        hdfs_disk_space = {}
        host_operating_system = {}
        rack_info = {}

        for host_entry in self.host_list :
            hosts_list_api_response = self.send_ambari_request("/hosts/" + host_entry)
            total_mem[host_entry] = hosts_list_api_response['Hosts']['total_mem']
            total_vcpu[host_entry] = hosts_list_api_response['Hosts']['cpu_count']
            total_pcpu[host_entry] = hosts_list_api_response['Hosts']['ph_cpu_count']
            rack_info[host_entry] = hosts_list_api_response['Hosts']['rack_info']
            host_operating_system[host_entry] = hosts_list_api_response['Hosts']['os_type'] + " " + hosts_list_api_response['Hosts']['os_arch']
            try:
                hdfs_disk_space_response = self.send_ambari_request("/hosts/" + host_entry + "/host_components/DATANODE")
                #log.debug(hdfs_disk_space_response)
                #hdfs_disk_space[host_entry] = hdfs_disk_space_response['metrics']['dfs']['FSDatasetState']['Capacity']
            except TypeError as e:
                pass

        dump_json(os.path.join(self.api_output_dir, "hosts_memory.json"), total_mem)
        dump_json(os.path.join(self.api_output_dir, "hosts_vcores.json"), total_vcpu)
        dump_json(os.path.join(self.api_output_dir, "hosts_physical_cpu.json"), total_pcpu)
        dump_json(os.path.join(self.api_output_dir, "hosts_operating_system.json"), host_operating_system)
        dump_json(os.path.join(self.api_output_dir, "hosts_hdfs_disk_space.json"), hdfs_disk_space)
        dump_json(os.path.join(self.api_output_dir, "hosts_rack_info.json"), rack_info)

    def collect_kerberos_info(self):
        log.debug("Read service information.")
        kerberos_info = {}
        kerberos_info_api_response_raw = self.send_ambari_request(
            "/configurations/service_config_versions?service_name=KERBEROS")
        kerberos_info_api_response = kerberos_info_api_response_raw['items'][
            len(kerberos_info_api_response_raw['items']) - 1]
        for x in kerberos_info_api_response.items():
            if x[0] == "configurations":
                for y in range(len(x[1])):
                    for z in (x[1][y]).items():
                        if z[0] == 'type' and z[1] == 'kerberos-env':
                            kerberos_info = x[1][y]['properties']
        dump_json(os.path.join(self.api_output_dir, "kerberos_info.json"), kerberos_info)
