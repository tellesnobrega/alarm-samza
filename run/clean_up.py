#!/usr/bin/python

import re
import sys
import pycurl
import cStringIO 
import json
import paramiko
import zc.zk

def _get_app_dict():
    app_dict = cStringIO.StringIO()
    c = pycurl.Curl()
    c.setopt(c.URL, "telles-samza-master:8088/ws/v1/cluster/apps")
    c.setopt(c.WRITEFUNCTION, app_dict.write)
    c.perform()
    return(json.loads(app_dict.getvalue()))

def stop_running_jobs(client):
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # probably want to remove this line after you have a file of host keys you trust
    client.connect("telles-samza-master",username="ubuntu", key_filename="/home/tellesmvn/Downloads/telles.pem")
    app_dict = _get_app_dict()['apps']['app']
    for app in app_dict:
        if (app['state'] == 'RUNNING'):
            print(app['id'])
            client.exec_command("alarm-samza/deploy/samza/bin/kill-yarn-job.sh %s" % (app['id']))
        
def delete_kafka_logs(client):
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # probably want to remove this line after you have a file of host keys you trust
    client.connect("telles-samza-master",username="ubuntu", key_filename="/home/tellesmvn/Downloads/telles.pem")
    client.exec_command("rm -rf /tmp/kafka-logs")

def delete_kafka_topics():
    zk = zc.zk.ZooKeeper('telles-samza-master:2181')
    zk.delete_recursive('brokers/topics') 
    zk.delete_recursive('config/topics')


def main(args):
    client = paramiko.SSHClient()
    stop_running_jobs(client)
    delete_kafka_logs(client)
    delete_kafka_topics()


if __name__ == "__main__":
  main(sys.argv[1:])
