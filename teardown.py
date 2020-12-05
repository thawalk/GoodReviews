import boto3
import os
from dotenv import load_dotenv
import ast
import credentials
import time

# private_dns = ['dfszxdvcsa']

# allnodes_ips = ['172.456.780']
# for instance_ip in allnodes_ips:
#     success = False
#     while(not success):
#         try:
#             for i in range(0, len(private_dns)):
#                 print('------------------inside the for loop-------------------')
#                 print(i)
#             success = True
#         except:
#             print('something went wrong, sleeping for a bit before retrying')
#             time.sleep(20)

# allnodes_private_ips = ['172.456.780']
# workers = open('./analytics_generated_items/workers', 'w')

# for private_ip in allnodes_private_ips:
#   workers.write('{}\n'.format(private_ip))
# workers.close()

# instance_node_list=['i-060b9afd15c41dd7c', 'i-050a52c2de7dfe54e', 'i-0f4b62fbe5c1400b4', 'i-01a1855933c639c34']
# security_group_id='sg-00cf46bdd463723a2'
# key_pair='hadoop_spark'

# environment_production_file = open("environment_production.py", 'w')
# environment_production_file.write('ec2_ids={}\n'.format(instance_node_list))
# environment_production_file.write('security_groups={}\n'.format([security_group_id]))
# environment_production_file.write('key_pair=\'{}\''.format(key_pair))
# environment_production_file.close()


# load_dotenv()


# session = boto3.session.Session(
#     aws_access_key_id=credentials.aws_access_key_id,
#     aws_secret_access_key=credentials.aws_secret_access_key,
#     aws_session_token=credentials.aws_session_token,
#     region_name=credentials.region_name
#     )

# ec2_resource = session.resource('ec2')

# # retrieve variables from env file
# ids_env = os.getenv("ec2_ids")
# ids = ast.literal_eval(ids_env)

# security_grps_env = os.getenv('security_groups')
# security_grps = ast.literal_eval(security_grps_env)

# key_pair = os.getenv('key_pair')

# # terminate the instances
# ec2_resource.instances.filter(InstanceIds = ids).terminate()

# print("Waiting for instances  to terminate")
# time.sleep(10)

# ec2 = boto3.client(
#     'ec2',
#     aws_access_key_id=credentials.aws_access_key_id,
#     aws_secret_access_key=credentials.aws_secret_access_key,
#     aws_session_token=credentials.aws_session_token,
#     region_name=credentials.region_name
#     )

# # delete the key
# delete_key =  ec2.delete_key_pair(KeyName=key_pair)

# # remove the security groups
# for sgid in security_grps:
#     print(sgid)
#     delete_sg = ec2.delete_security_group(GroupId=sgid)


print('--------------------------------donesies----------------------------------')
# print(ec2)
