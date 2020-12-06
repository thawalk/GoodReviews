import boto3
import sys, os
from dotenv import load_dotenv
import ast
import environment_hadoop
sys.path.append('../')
import credentials
import time



session = boto3.session.Session(
    aws_access_key_id=credentials.aws_access_key_id,
    aws_secret_access_key=credentials.aws_secret_access_key,
    aws_session_token=credentials.aws_session_token,
    region_name=credentials.region_name
    )

ec2_resource = session.resource('ec2')


ids = environment_hadoop.ec2_ids
security_grps = environment_hadoop.security_groups
key_pair = environment_hadoop.key_pair


ec2 = boto3.client(
    'ec2',
    aws_access_key_id=credentials.aws_access_key_id,
    aws_secret_access_key=credentials.aws_secret_access_key,
    aws_session_token=credentials.aws_session_token,
    region_name=credentials.region_name
    )
waiter = ec2.get_waiter('instance_terminated')

# terminate the instances
# ec2_resource.instances.filter(InstanceIds = ids).terminate()
ec2.terminate_instances(InstanceIds = ids)
print("Waiting for instances to terminate")
waiter.wait(InstanceIds=ids)
# time.sleep(10)

# # delete the key
delete_key =  ec2.delete_key_pair(KeyName=key_pair)

# # remove the security groups
for sgid in security_grps:
    print(sgid)
    delete_sg = ec2.delete_security_group(GroupId=sgid)

open('../hadoop/excludes', 'w').close()

print('--------------------------------done tearing down the Hadoop ec2s----------------------------------')