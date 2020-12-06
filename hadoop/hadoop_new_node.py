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

specified_num_nodes = int(input('Please key in the number of nodes you want to commission:'))



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
private_namenode_ip = scaling.private_namenode_ip
older_public_ips = scaling.public_ips
older_datanode_public_ips = scaling.datanode_ips
older_private_ips = scaling.private_ips
older_private_datanode_dns_list = scaling.private_datanode_dns_list
older_instance_node_list = scaling.older_instance_node_list
older_data_node_ips = scaling.older_data_node_ips



tier = 't2.xlarge'
instance_ami = 'ami-00ddb0e5626798373'

# ----------------------------- setting the ec2 config and creating it ------------------------------------------->
instance_node_list = analytics_functions.create_instances_hadoop(
    instance_ami, specified_num_nodes, tier, key_pair, security_group_id,ec2)


time.sleep(3)

instance_dic = analytics_functions.list_ec2_instances(ec2)
private_instance_dic = analytics_functions.list_private_ec2_instances(ec2)
dns_dic = analytics_functions.get_publicdns(ec2)


new_node_ips = []
new_node_private_ips = []

for i in instance_node_list:
    new_node_ips.append(instance_dic[i])
    # new_node_dns.append(dns_dic[i])

for i in instance_node_list:
    new_node_private_ips.append(private_instance_dic[i])

olderForLoopPublicIPS= older_public_ips

total_new_public_ip_list = older_public_ips + new_node_ips

total_new_private_ip_list = older_private_ips + new_node_private_ips

new_instance_node_list = older_instance_node_list + instance_node_list


new_data_node_ips = older_data_node_ips + new_node_ips
private_dns = []






# ----------------------------------------- writing to file to help with teardown -------------------------------------

environment_hadoop_file = open("../teardown/environment_hadoop.py", 'w')
environment_hadoop_file.write('ec2_ids={}\n'.format(new_instance_node_list))
environment_hadoop_file.write('security_groups={}\n'.format([security_group_id]))
environment_hadoop_file.write('key_pair=\'{}\''.format(key_pair))
environment_hadoop_file.close()

# -------------------------------------- writing to hosts file ----------------------------------------------------

hosts_file = open("../all_nodes_deploy/hosts", 'w')
hosts_file.write('127.0.0.1 localhost\n')
hosts_file.write('\n')
for i in range(len(total_new_private_ip_list)):
    if(i == 0):
        hosts_file.write('{} n0\n'.format(total_new_private_ip_list[i]))
    else:
        hosts_file.write('{} d{}\n'.format(total_new_private_ip_list[i], i))
        private_dns.append('d{}'.format(i))
hosts_file.write('\n')
hosts_file.write(
    '# The following lines are desirable for IPv6 capable hosts\n')
hosts_file.write('::1 ip6-localhost ip6-loopback\n')
hosts_file.write('fe00::0 ip6-localnet\n')
hosts_file.write('ff00::0 ip6-mcastprefix\n')
hosts_file.write('f02::1 ip6-allnodes\n')
hosts_file.write('ff02::2 ip6-allrouters\n')
hosts_file.write('ff02::3 ip6-allhosts\n')
hosts_file.close()

# -------------------------------------- writing to includes file ----------------------------------------------------

includes = open("includes", 'w')
for private_ip in total_new_private_ip_list:
    if private_ip != private_namenode_ip:
        includes.write('{}\n'.format(private_ip))
includes.close()


# ----------------- writing to a scaling file to keep track of config, to help with commissioning and decommissioning nodes after cluster is up -------------- >
scaling_file = open("scaling.py",'w')
scaling_file.write('namenode_ip=\'{}\'\n'.format(public_namenode_ip))
scaling_file.write('datanode_ips={}\n'.format(new_data_node_ips))
scaling_file.write('public_ips={}\n'.format(total_new_public_ip_list))
scaling_file.write('private_ips={}\n'.format(total_new_private_ip_list))
scaling_file.write('private_namenode_ip=\'{}\'\n'.format(private_namenode_ip))
scaling_file.write('private_datanode_dns_list={}\n'.format(private_dns))
scaling_file.write('security_group_id=\'{}\'\n'.format(security_group_id))
scaling_file.write('key_pair=\'{}\'\n'.format(key_pair))
scaling_file.write('older_instance_node_list={}\n'.format(new_instance_node_list))
scaling_file.write('older_data_node_ips={}\n'.format(new_data_node_ips))
scaling_file.close()





# -------------------------------------- writing to workers file ----------------------------------------------------

workers = open('../all_nodes_deploy/workers', 'w')
for private_ip in total_new_private_ip_list:
    if private_ip != private_namenode_ip:
        workers.write('{}\n'.format(private_ip))
workers.close()

print("Waiting for instances to start up")
time.sleep(120)


# ---------------------------------- update the packages on the new data nodes------------------------------------------- >

print("------------------------- Updating the packages on the new data nodes --------------------------------------")



# update the packages only on the new nodes
for instance_ip in new_node_ips:
    success = False
    tryfactor = 0
    while(not success):
        try:
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.sudo('apt-get -y update')
            success = True

        except:
            # in case fail
            print('something went wrong, retrying in a moment')
            tryfactor += 1
            if tryfactor == 10:
                print('It has been {} times, something went horribly wrong. Ctrl C to exit and try again'.format(
                    tryfactor))
            time.sleep(10)


# ------------------------------------------- reboot ----------------------------------------------------
print("------------------------- Rebooting --------------------------------------")


try:
    print(instance_node_list)
    print('---------------trying to reboot---------------')
    ec2.reboot_instances(InstanceIds=instance_node_list, DryRun=True)
except ClientError as e:
    if 'DryRunOperation' not in str(e):
        print(str(e))
        print("You don't have permission to reboot instances.")
        raise

try:
    response = ec2.reboot_instances(
        InstanceIds=instance_node_list, DryRun=False)
    print('Success', response)
except ClientError as e:
    print('Error', e)

# Wait for instances to reboot for 60s
print("Waiting for Reboot")
time.sleep(60)

# -------------------------------------------------------------- SSH -------------------------------------------------------->


print('--------------------------------------Adding the ssh key to the new datanodes------------------------------')

# # Using the Namenode's key, put it up into the new Datanodes and cat to authorized
for instance_ip in new_node_ips:
    success = False
    while(not success):
        try:
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.put('id_rsa.pub')
            c.sudo('cat id_rsa.pub >> ~/.ssh/authorized_keys')
            success = True
        except:
            print('something went wrong, sleeping for a bit before retrying')
            time.sleep(20)



print('--------------------------------------done with the ssh stuff------------------------------')
# --------------------------------------------------------end of ssh stuff-------------------------------------------------//

# ----------------------------------- Loop through the new data nodes and set up hadoop -------------------------- > 
print("------------------------- Setting up the new data nodes --------------------------------------")


JH = "\/usr\/lib\/jvm\/java-8-openjdk-amd64"

for instance_ip in new_node_ips:
    success = False
    while(not success):
        try:
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.sudo('apt-get -y update')
            c.put('../all_nodes_deploy/hosts')
            c.sudo('mv hosts /etc/hosts')
            print('--------------------------------done putting hosts----------------------------')
            c.run('ssh-keyscan -H n0 >> ~/.ssh/known_hosts')
            for i in range(0, len(private_dns)):
                print('------------------inside the for loop-------------------')
                print(i)
                c.run('ssh-keyscan -H d{} >> ~/.ssh/known_hosts'.format(i + 1))
            c.sudo('sysctl vm.swappiness=10')
            print('--------------------------------swappiness works----------------------------')
            c.sudo('apt-get install -y openjdk-8-jdk')
            print('-----------------------------install java------------------------------')
            c.run('mkdir download')
            c.run('cd download && wget https://apachemirror.sg.wuchna.com/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz')
            print('----------------------------------------step 1 done---------------------------------------')
            c.run('cd download && tar zxvf hadoop-3.3.0.tar.gz')
            print('----------------------------------------step 2 done---------------------------------------')
            c.run('export JH="\/usr\/lib\/jvm\/java-8-openjdk-amd64"')
            print('----------------------------------------step 3 done---------------------------------------')
            c.run('rm /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/workers')
            c.put('../all_nodes_deploy/workers')
            c.sudo('mv workers /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('--------------------------------done putting workers----------------------------')
            c.run('sed -i "s/# export JAVA_HOME=.*/export\ JAVA_HOME={}/g" /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/hadoop-env.sh'.format(JH))
            print('----------------------------------------step 4 done---------------------------------------')
            c.put('../all_nodes_deploy/hdfs-site.xml')
            c.sudo('mv hdfs-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 7 done---------------------------------------')
            c.put('../all_nodes_deploy/mapred-site.xml')
            c.sudo('mv mapred-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 8 done---------------------------------------')
            c.put('../all_nodes_deploy/core-site.xml')
            c.sudo('mv core-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 9 done---------------------------------------')
            c.put('../all_nodes_deploy/yarn-site.xml')
            c.sudo('mv yarn-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 10 done---------------------------------------')
            c.sudo('mv /home/ubuntu/download/hadoop-3.3.0 /opt/')
            c.sudo('mkdir -p /mnt/hadoop/datanode/')
            c.sudo('chown -R ubuntu:ubuntu /mnt/hadoop/datanode')
            success = True
        except:
            print('something went wrong, sleeping for a bit before retrying')
            time.sleep(20)


print('-------------------------------------------Done setting up the new data nodes-----------------------------------------------')

# for instance_ip in new_node_ips:
#   c = analytics_functions.theconnector(instance_ip, key_pair)
#   c.sudo('mkdir -p /mnt/hadoop/datanode/')
#   c.sudo('chown -R ubuntu:ubuntu /mnt/hadoop/datanode')


# ------------------------- Loop through the older nodes and inform them about the new nodes-------------------------- > 

print("------------------------- Informing the older nodes about the new nodes --------------------------------------")


for instance_ip in olderForLoopPublicIPS:
    success = False
    while(not success):
        try:
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.sudo('apt-get -y update')
            c.put('../all_nodes_deploy/hosts')
            c.sudo('mv hosts /etc/hosts')
            for i in range(1, len(new_node_ips) + 1):
                c.run('ssh-keyscan -H {} >> ~/.ssh/known_hosts'.format(private_dns[-i]))
            c.run('rm /opt/hadoop-3.3.0/etc/hadoop/workers')
            c.put('../all_nodes_deploy/workers')
            c.sudo('mv workers /opt/hadoop-3.3.0/etc/hadoop/')
            success = True
        except:
            print('something went wrong, sleeping for a bit before retrying')
            time.sleep(20)

print("------------------------- Done informing the older nodes about the new nodes --------------------------------------")

# ------------------------- Connect to the namenode and refresh the cluster-------------------------- > 

print("---------- updating the  cluster-----------")

c = analytics_functions.theconnector(public_namenode_ip, key_pair)

c.run('rm includes')
c.put('./includes')
c.run('rm excludes')
c.put('./excludes')
c.run('/opt/hadoop-3.3.0/sbin/stop-all.sh')
c.run('/opt/hadoop-3.3.0/sbin/start-dfs.sh && /opt/hadoop-3.3.0/sbin/start-yarn.sh')
c.run('/opt/hadoop-3.3.0/bin/hdfs dfsadmin -report')
c.close()

print("----------Done adding the nodes! Scroll up to see the report for the updated cluster-----------")