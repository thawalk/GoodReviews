import boto3
from fabric import Connection
import os, sys
import time
from botocore.exceptions import ClientError
import scaling
sys.path.append('../')
import credentials
import analytics_functions


aws_access_key_id = input('Please key in your AWS access key ID: ')

aws_secret_access_key = input('Please key in your AWS secret access key: ')

aws_session_token =input('Please key in your session token: ')

specified_num_nodes = int(input('Please key in the number of nodes you want to decommission:'))

region_name='us-east-1'

# python 2
credentials_file = open("../credentials.py", 'w')
credentials_file.write('aws_access_key_id=\'{}\'\n'.format(aws_access_key_id))
credentials_file.write('aws_secret_access_key=\'{}\'\n'.format(aws_secret_access_key))
credentials_file.write('aws_session_token=\'{}\'\n'.format(aws_session_token))
credentials_file.write('region_name=\'{}\'\n'.format(region_name))
credentials_file.close()

ec2 = boto3.client(
    'ec2', 
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=region_name
)



analytics_functions.list_ec2_instances(ec2)


# <------------------------------- getting the necessary information from scaling file--------------------------------->

key_pair = scaling.key_pair
security_group_id = scaling.security_group_id
public_namenode_ip = scaling.namenode_ip
older_private_ips =scaling.private_ips



node_to_remove_private_ip_list = []
for i in range(1,specified_num_nodes + 1):
    node_to_remove_private_ip_list.append(older_private_ips[-i])


excludes = open("excludes", 'a')
for i in range(specified_num_nodes):
    excludes.write('{}\n'.format(node_to_remove_private_ip_list[i]))
excludes.close()




print("----------- Connecting to the namenode to decommission -----------")


c = analytics_functions.theconnector(public_namenode_ip,key_pair)
c.run('rm includes')
c.put('./includes')
c.run('rm excludes')
c.put('./excludes')

c.run('/opt/hadoop-3.3.0/bin/yarn rmadmin -refreshNodes')
c.run('/opt/hadoop-3.3.0/bin/hdfs dfsadmin -refreshNodes')
c.run('/opt/hadoop-3.3.0/bin/hdfs dfsadmin -report')
# c.run('/opt/hadoop-3.3.0/bin/hadoop balancer')
c.close()

print("----------Done decommissioning the nodes! Scroll up to see the report for the updated cluster-----------")
# ./hadoop-daemon.sh start datanode