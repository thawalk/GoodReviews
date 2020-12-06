import boto3
from fabric import Connection
import os, sys
import time
from botocore.exceptions import ClientError
from dotenv import load_dotenv
sys.path.append('../')
# import credentials
import analytics_functions




aws_access_key_id = input('Please key in your AWS access key ID: ')

aws_secret_access_key = input('Please key in your AWS secret access key: ')

aws_session_token =input('Please key in your session token: ')

specified_num_nodes = int(input('Please key in the number of nodes for the cluster:'))



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


# --------------------------------- setting security group config and making the security group --------------------- >
security_group_name = 'hadoop_spark'
security_group_description = 'hadoop_spark'
security_group_id = analytics_functions.create_hadoop_security_group(
    security_group_name, security_group_description, ec2)

# ------------------------------- setting the key name and creating it ------------------------------------------ >
key_pair = 'hadoop_spark'
analytics_functions.create_key_pair(key_pair, ec2)

# specified_num_nodes = 2


# ----------------------------- setting the ec2 config and creating it ------------------------------------------->

# -----CHANGE THIS----
tier = 't2.xlarge'
instance_ami = 'ami-00ddb0e5626798373'  # base image for Ubuntu 18.04 LTS

instance_node_list = analytics_functions.create_instances_hadoop(
    instance_ami, specified_num_nodes, tier, key_pair, security_group_id, ec2)


# setting the first instance as the name node
namenode_id = instance_node_list[0]

# set others as datanodes
datanode_ids = instance_node_list[1:]

time.sleep(3)


# retrieving all the necessary ips
instance_dic = analytics_functions.list_ec2_instances(ec2)
private_instance_dic = analytics_functions.list_private_ec2_instances(ec2)
dns_dic = analytics_functions.get_publicdns(ec2)

allnodes_ips = []
allnodes_dns = []
allnodes_private_ips = []
private_dns = []
datanode_ips = []
datanode_dns = []

for i in instance_node_list:
    allnodes_ips.append(instance_dic[i])
    allnodes_dns.append(dns_dic[i])

for i in instance_node_list:
    allnodes_private_ips.append(private_instance_dic[i])


for i in datanode_ids:
    datanode_ips.append(instance_dic[i])
    datanode_dns.append(dns_dic[i])

namenode_ip = instance_dic[namenode_id]
namenode_dns = dns_dic[namenode_id]
private_namenode_ip = private_instance_dic[namenode_id]

# # ----------------------------------------- writing to environment_hadoop file to help with teardown ------------- > 

environment_hadoop_file = open("../teardown/environment_hadoop.py", 'w')
environment_hadoop_file.write('ec2_ids={}\n'.format(instance_node_list))
environment_hadoop_file.write('security_groups={}\n'.format([security_group_id]))
environment_hadoop_file.write('key_pair=\'{}\''.format(key_pair))
environment_hadoop_file.close()

# -------------------------------------- writing to hosts -------------------------------------------------------- > 
hosts_file = open("../all_nodes_deploy/hosts", 'w')
hosts_file.write('127.0.0.1 localhost\n')
hosts_file.write('\n')
for i in range(len(allnodes_private_ips)):
    if(i == 0):
        hosts_file.write('{} n0\n'.format(allnodes_private_ips[i]))
    else:
        hosts_file.write('{} d{}\n'.format(allnodes_private_ips[i], i))
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


# -------------------------------------- writing to workers file ---------------------------------------------------- > 
workers = open('../all_nodes_deploy/workers', 'w')
for private_ip in allnodes_private_ips:
    if private_ip != private_namenode_ip:
        workers.write('{}\n'.format(private_ip))
workers.close()



# ----------------- writing to a scaling file to keep track of config, to help with commissioning and decommissioning nodes after cluster is up -------------- >
scaling_file = open("scaling.py",'w')
scaling_file.write('namenode_ip=\'{}\'\n'.format(namenode_ip))
scaling_file.write('datanode_ips={}\n'.format(datanode_ips))
scaling_file.write('public_ips={}\n'.format(allnodes_ips))
scaling_file.write('private_ips={}\n'.format(allnodes_private_ips))
scaling_file.write('private_namenode_ip=\'{}\'\n'.format(private_namenode_ip))
scaling_file.write('private_datanode_dns_list={}\n'.format(private_dns))
scaling_file.write('security_group_id=\'{}\'\n'.format(security_group_id))
scaling_file.write('key_pair=\'{}\'\n'.format(key_pair))
scaling_file.write('older_instance_node_list={}\n'.format(instance_node_list))
scaling_file.write('older_data_node_ips={}\n'.format(datanode_ips))
scaling_file.close()


print(" ----------------------------- Waiting for instances to start up ---------------------------------")
time.sleep(80)

# ---------------------------------- update the packages ------------------------------------------- >
for instance_ip in allnodes_ips:
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

# ------------------------------------------- reboot ---------------------------------------------------- >

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
print('-------------------------------------- starting to make the public key in the name node ------------------------------')

# creating an SSH key pair in our namenode
success = False
while(not success):
    try:
        # Connect to Namenode and generate key
        c = analytics_functions.theconnector(namenode_ip, key_pair)
        c.sudo('apt-get install -y ssh')
        c.run('ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa')
        success = True
    except:
        # in case fail
        print('something went wrong, sleeping for a bit before retrying')
        time.sleep(20)

# get the public key from namenode
c.get('.ssh/id_rsa.pub')

# Using the Namenode's key, put it up into the Datanodes and cat to authorized
for instance_ip in datanode_ips:
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

# We cat into the namenode's authorized keys as well
success = False
while(not success):
    try:
        # cat to namenode authorized keys as well
        c = analytics_functions.theconnector(namenode_ip, key_pair)
        c.run('cd .ssh && cat id_rsa.pub >> ~/.ssh/authorized_keys')
        success = True
    except:
        print('something went wrong, sleeping for a bit before retrying')
        time.sleep(20)


print('-------------------------------------- done with adding ssh key to all the nodes ------------------------------')

# --------------------------------------------------------end of ssh stuff------------------------------------------------- >

# ---------------------------- writing the private ip to config files for hadoop, such yarn, core-site -------------------------------------->
template = '../all_nodes_templates/core-site-template.xml'
output_file = '../all_nodes_deploy/core-site.xml'
term_to_change = '<nnode>'
new_term = private_namenode_ip

analytics_functions.replace_single_term(
    template, output_file, term_to_change, new_term)

template = '../all_nodes_templates/yarn-site-template.xml'
output_file = '../all_nodes_deploy/yarn-site.xml'
term_to_change = '<nnode>'
new_term = private_namenode_ip

analytics_functions.replace_single_term(
    template, output_file, term_to_change, new_term)

# ----------------- special config for namenode config because of the includes and excludes used for commissioning and decommissioning --------------------------------->

template = '../name_node_scaling_template/yarn-site-template.xml'
output_file = '../name_node_deploy/yarn-site.xml'
term_to_change = '<nnode>'
new_term = private_namenode_ip

analytics_functions.replace_single_term(
    template, output_file, term_to_change, new_term)

# ----------------------------------------- hadoop configuration for the namenode -------------------------------------------------->
print('-------------------------------------------- starting the hadoop installation -------------------------------------------')

JH = "\/usr\/lib\/jvm\/java-8-openjdk-amd64"

for instance_ip in [namenode_ip]:
    success = False
    while(not success):
        try:
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.sudo('apt-get -y update')
            c.put('../all_nodes_deploy/hosts')
            c.sudo('mv hosts /etc/hosts')
            c.run('touch excludes')
            c.run('touch includes')
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
            c.put('../name_node_deploy/hdfs-site.xml')
            c.sudo('mv hdfs-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 7 done---------------------------------------')
            c.put('../all_nodes_deploy/mapred-site.xml')
            c.sudo('mv mapred-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 8 done---------------------------------------')
            c.put('../all_nodes_deploy/core-site.xml')
            c.sudo('mv core-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 9 done---------------------------------------')
            c.put('../name_node_deploy/yarn-site.xml')
            c.sudo('mv yarn-site.xml /home/ubuntu/download/hadoop-3.3.0/etc/hadoop/')
            print('----------------------------------------step 10 done---------------------------------------')
            c.sudo('mv /home/ubuntu/download/hadoop-3.3.0 /opt/')
            c.sudo('sudo mkdir -p /mnt/hadoop/namenode/hadoop-ubuntu')
            c.sudo('sudo chown -R ubuntu:ubuntu /mnt/hadoop/namenode')
            success = True
        except:
            print('something went wrong, sleeping for a bit before retrying')
            time.sleep(20)


# ----------------------------------- Loop through the data nodes and set up hadoop -------------------------- > 

for instance_ip in datanode_ips:
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


print('-------------------------------------------done with the hadoop installation------------------------------------------------')

# #  ------------------------------------------ start the hadoop cluster ------------------------------------------------------- >

print('-------------------------------------------- starting the pysqoop installation -------------------------------------------')

c = analytics_functions.theconnector(namenode_ip, key_pair)
c.run('yes Y |/opt/hadoop-3.3.0/bin/hdfs namenode -format')
c.run('/opt/hadoop-3.3.0/sbin/start-dfs.sh && /opt/hadoop-3.3.0/sbin/start-yarn.sh')


# # ---------------------------------------- installing pysqoop in the namenode ----------------------------------------------------

print('-------------------------------------------- starting the pysqoop installation -------------------------------------------')

HD ="\/opt\/hadoop-3.3.0"

c.run('wget https://apachemirror.sg.wuchna.com/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz')
print('----------------------------------------step 1 done---------------------------------------')
c.run('tar zxvf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz')
print('----------------------------------------step 2 done---------------------------------------')
c.run('cp sqoop-1.4.7.bin__hadoop-2.6.0/conf/sqoop-env-template.sh sqoop-1.4.7.bin__hadoop-2.6.0/conf/sqoop-env.sh')
print('----------------------------------------step 3 done---------------------------------------')
c.run('export HD="\/opt\/hadoop-3.3.0"')
print('----------------------------------------step 4 done---------------------------------------')
c.run('sed -i "s/#export HADOOP_COMMON_HOME=.*/export HADOOP_COMMON_HOME={}/g" sqoop-1.4.7.bin__hadoop-2.6.0/conf/sqoop-env.sh'.format(HD))
print('----------------------------------------step 5 done---------------------------------------')
c.run('sed -i "s/#export HADOOP_MAPRED_HOME=.*/export HADOOP_MAPRED_HOME={}/g" sqoop-1.4.7.bin__hadoop-2.6.0/conf/sqoop-env.sh'.format(HD))
print('----------------------------------------step 6 done---------------------------------------')
c.run('wget https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar')
print('----------------------------------------step 7 done---------------------------------------')
c.run(' cp commons-lang-2.6.jar sqoop-1.4.7.bin__hadoop-2.6.0/lib/')
print('----------------------------------------step 8 done---------------------------------------')
c.sudo('cp -rf sqoop-1.4.7.bin__hadoop-2.6.0 /opt/sqoop-1.4.7')
print('----------------------------------------step 9 done---------------------------------------')
c.sudo('apt install libmysql-java')
print('----------------------------------------step 10 done---------------------------------------')
c.sudo('ln -snvf /usr/share/java/mysql-connector-java.jar /opt/sqoop-1.4.7/lib/mysql-connector-java.jar')
print('----------------------------------------step 11 done---------------------------------------')
c.run('export PATH=$PATH:/opt/sqoop-1.4.7/bin')
print('----------------------------------------step 12 done---------------------------------------')
c.run('echo "PATH=$PATH:/opt/sqoop-1.4.7/bin" >> ~/.bashrc')
c.run('source ~/.bashrc')
c.run('echo "PATH=$PATH:/opt/hadoop-3.3.0/sbin:/opt/hadoop-3.3.0/bin" >>  ~/.bashrc')
c.run('source ~/.bashrc')
c.run('echo "export PATH=$PATH:/opt/hadoop-3.3.0/bin" >> ~/.bash_profile')
print('----------------------------------------step 13 done---------------------------------------')

print('-------------------------------------------- Done with the pysqoop installation -------------------------------------------')


# # -------------------------------------------------------------------- Ingesting the data ----------------------------------------------------------




load_dotenv()
mysql_ip = os.getenv('host')
c.run('/opt/sqoop-1.4.7/bin/sqoop import-all-tables --fields-terminated-by "\t" --connect jdbc:mysql://{}/reviews?useSSL=false --username sqoop --password sqoop123'.format(mysql_ip))

mongo_ip = os.getenv('mongo_url')
c.run('wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1804-x86_64-100.2.1.deb')
c.run('sudo apt install ./mongodb-database-tools-*-100.2.1.deb')
c.run('mongoexport --uri="mongodb://{}:27017/test" --collection=test_collection --out=metadata.json'.format(mongo_ip))
c.run('/opt/hadoop-3.3.0/bin/hdfs dfs -mkdir metadata')
c.run('/opt/hadoop-3.3.0/bin/hdfs dfs -put ./metadata.json metadata')

# # changing the placeholder on the template to the namenode ip 
analytics_functions.replace_single_term('../analytics_files/pearson_template.py', '../analytics_files/pearson.py', 'namenode_ip', private_namenode_ip)
analytics_functions.replace_single_term('../analytics_files/tfidf_template.py', '../analytics_files/tfidf.py', 'namenode_ip', private_namenode_ip)




# ---------------------------------------- installing pyspark and putting in the analytics file----------------------------------------- //

# c = analytics_functions.theconnector('54.86.67.155', 'hadoop_spark')
c.sudo('apt -y install python3-pip')
c.run('pip3 install pyspark')
c.put('../analytics_files/pearson.py')
c.put('../analytics_files/tfidf.py')
c.close()

# # ----------------------------------------- Install Spark---------------------------------------------------
# # allnodes_ips = ['34.207.156.153', '34.227.90.103', '18.233.64.12', '34.201.164.193']
# # allnodes_private_ips = ['172.31.53.92', '172.31.50.110', '172.31.49.159', '172.31.53.196']
# # private_namenode_ip = '172.31.53.92'
# # namenode_ip = '34.207.156.153'

print('-------------------------------------------- starting the spark installation -------------------------------------------')

slaves = open('../spark_config/slaves', 'w')
for private_ip in allnodes_private_ips:
    if private_ip != private_namenode_ip:
        slaves.write('{}\n'.format(private_ip))
slaves.close()

for instance_ip in allnodes_ips:
    success = False
    while(not success):
        try:
            print(instance_ip)
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.run('wget https://apachemirror.sg.wuchna.com/spark/spark-3.0.1/spark-3.0.1-bin-hadoop3.2.tgz')
            c.run('tar zxvf spark-3.0.1-bin-hadoop3.2.tgz')
            c.run('cp spark-3.0.1-bin-hadoop3.2/conf/spark-env.sh.template spark-3.0.1-bin-hadoop3.2/conf/spark-env.sh')
            c.put('../spark_config/spark-env.sh')
            c.sudo('mv spark-env.sh spark-3.0.1-bin-hadoop3.2/conf/')
            c.put('../spark_config/slaves')
            c.sudo('mv slaves spark-3.0.1-bin-hadoop3.2/conf/')
            c.sudo('mv spark-3.0.1-bin-hadoop3.2 /opt/')
            c.sudo('chown -R ubuntu:ubuntu /opt/spark-3.0.1-bin-hadoop3.2')
            c.run('echo "PATH=$PATH:/opt/spark-3.0.1-bin-hadoop3.2/bin" >>  ~/.bashrc')
            c.run('source ~/.bashrc')
            success = True
        except:
            print('something went wrong, sleeping for a bit before retrying')
            time.sleep(20)

print('-------------------------------------------- Done with the spark installation -------------------------------------------')

# ------------------------------- connect to the namenode, start spark cluster ----------------------------------------

print('-------------------------------------------- starting the spark cluster -------------------------------------------')

c = analytics_functions.theconnector(namenode_ip,key_pair)

c.run('/opt/spark-3.0.1-bin-hadoop3.2/sbin/start-all.sh')
c.run('/opt/hadoop-3.3.0/bin/hdfs dfsadmin -report')
c.run('jps')

c.close()


print("--------------------------Done, hadoop and spark cluster is running with the data ingested -----------------------")
print("-------------------------Scroll up to see the hdfs report and jps after spark is installed-----------------------")


# print('----------------------------------namenode stuff---------------------------')
# print('-------------------------------------------------------------------------')
# print('----------------------------------namenode dns-------------------------')
# print(namenode_dns)
# print('----------------------------------namenode ip---------------------------')
# print(namenode_ip)
# print('-----------------------------------namenode private ip------------------')
# print(private_namenode_ip)
# print('-----------------------------------datanodes stuff----------------------')
# print('-------------------------------------------------------------------------')
# print('-----------------------------------datanodes public ips---------------------')
# print(datanode_ips)
# print('-----------------------------------all nodes private ips---------------------')
# print('-----------it includes the namenode private ip, just compare with the above one----------')
# print(allnodes_private_ips)
# print('-----------------------------------end of info------------------------------------')

