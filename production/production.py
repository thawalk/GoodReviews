import boto3
import os, sys
import time
from fabric import Connection
from botocore.exceptions import ClientError
sys.path.append('../')
import credentials
import analytics_functions

aws_access_key_id = input('Please key in your AWS access key ID: ')

aws_secret_access_key = input('Please key in your AWS secret access key: ')

aws_session_token =input('Please key in your session token: ')



region_name='us-east-1'


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
ec2 = boto3.client(
    'ec2', 
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name=region_name
    )
    

analytics_functions.list_ec2_instances(ec2)

# -------------------------------------------- Set all the security group config ------------------------------------------------

## MongoDB Permissions
mongo_permissions = [{'IpProtocol': 'tcp',
                   'FromPort': 22,
                   'ToPort': 22,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'SSH'}]
                    },
                  {'IpProtocol': '-1',
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'All'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 27017,
                   'ToPort': 27017,
                   'IpRanges': [{
                       'CidrIp':'0.0.0.0/0',
                       'Description': 'MongoDB'}]}]

## Mysql Permissions
mysql_permissions = [{'IpProtocol': 'tcp',
                   'FromPort': 22,
                   'ToPort': 22,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'SSH'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 80,
                   'ToPort': 80,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'HTTP'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 3306,
                   'ToPort': 3306,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'MySQL'}]}]

web_permissions = [{'IpProtocol': 'tcp',
                   'FromPort': 22,
                   'ToPort': 22,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'SSH'}]},
                  {'IpProtocol': '-1',
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'All'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 5000,
                   'ToPort': 5000,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'HTTP'}]},
                  {'IpProtocol': 'tcp',
                   'FromPort': 80,
                   'ToPort': 80,
                   'IpRanges': [{
                       'CidrIp': '0.0.0.0/0',
                       'Description': 'HTTP'}]}]                    



# -------------------------------------------- create the security groups ------------------------------------------------
mon_security_group_name = 'Mongo'
mon_des= 'Mongo'
mon_secure = analytics_functions.create_security_group(mon_security_group_name, mon_des, mongo_permissions, ec2)

mys_security_group_name = 'MySQL'
mys_des = 'MySQL'
mys_secure = analytics_functions.create_security_group(mys_security_group_name, mys_des, mysql_permissions, ec2)

web_security_group_name = 'Server'
web_des = 'Server'
web_secure = analytics_functions.create_security_group(web_security_group_name, web_des, web_permissions, ec2)



# -------------------------------------------- create the key_pair ------------------------------------------------
key_pair = 'mongo-sql-server'
# test to see if this key name already exists
analytics_functions.create_key_pair(key_pair, ec2)



# tier can be made bigger to make the whole process faster, example: 't2.medium'
tier = 't2.medium'
instance_ami = 'ami-00ddb0e5626798373' # using the basic image with ubuntu18.04LTS



# -------------------------------------------- getting the necessary IPs ------------------------------------------------

mongo_instance = analytics_functions.create_instances(instance_ami, 1, tier, key_pair, mon_secure, ec2, 'mongo')
mongo_node_id = mongo_instance[0]

mysql_instance = analytics_functions.create_instances(instance_ami, 1, tier, key_pair, mys_secure, ec2, 'mysql')
mysql_node_id = mysql_instance[0]

web_instance = analytics_functions.create_instances(instance_ami, 1, tier, key_pair, web_secure, ec2, 'server')
web_node_id = web_instance[0]

time.sleep(3)

instance_dic = analytics_functions.list_ec2_instances(ec2)
dns_dic = analytics_functions.get_publicdns(ec2)

mongo_ip = instance_dic[mongo_node_id]
print('mongo ip\n', mongo_ip)
mongo_dns = dns_dic[mongo_node_id]
print('mongo dns\n', mongo_dns)


mysql_ip = instance_dic[mysql_node_id]
print('mysql ip\n', mysql_ip)
mysql_dns = dns_dic[mysql_node_id]
print('mysql dns\n', mysql_dns)

web_ip = instance_dic[web_node_id]
print('web ip\n', web_ip)
web_dns = dns_dic[web_node_id]
print('web dns\n', web_dns)




all_node_ips = [mongo_ip, mysql_ip, web_ip]
all_node_ids = [mongo_node_id, mysql_node_id, web_node_id]
all_node_security_groups = [mon_secure, mys_secure, web_secure]




print("Waiting for instances  to start up")
time.sleep(60)


# ---------------------------------- update the packages ------------------------------------------- >


for instance_ip in all_node_ips:
    success = False
    while(not success):
        try: 
            c = analytics_functions.theconnector(instance_ip, key_pair)
            c.sudo('apt-get update')
            success = True

        except: 
            # in case fail
            print('something went wrong, retrying i a moment')
            time.sleep(10)


# ------------------------------------------- reboot ---------------------------------------------------- >

try:
    ec2.reboot_instances(InstanceIds=all_node_ids, DryRun=True)
except ClientError as e:
    if 'DryRunOperation' not in str(e):
        print("You don't have permission to reboot instances.")
        raise

try:
    response = ec2.reboot_instances(InstanceIds=all_node_ids, DryRun=False)
    print('Success', response)
except ClientError as e:
    print('Error', e)


time.sleep(60)

# ------------------------------------------- db config ---------------------------------------------------- >

name_of_db_meta_data = 'test'
name_of_collection_meta_data= 'test_collection'
name_of_db_user_logs='user_analytics'
name_of_collection_user_logs='logging'
mongo_url = '{}'.format(mongo_ip) # insert mongo url here



# ------------------------------------- writing to an environment file for teardown ------------------------------
environment_hadoop_file = open("../teardown/environment_production.py", 'w')
environment_hadoop_file.write('ec2_ids={}\n'.format(all_node_ids))
environment_hadoop_file.write('security_groups={}\n'.format(all_node_security_groups))
environment_hadoop_file.write('key_pair=\'{}\''.format(key_pair))
environment_hadoop_file.close()


# ------------------------------------ writing to env file for the flask server to use --------------------------
environment_file = open("../.env", 'w')
environment_file.write('database_name_meta_data={}\n'.format(name_of_db_meta_data))
environment_file.write('database_name_user_logs={}\n'.format(name_of_collection_meta_data))
environment_file.write('mongo_url={}\n'.format(mongo_url))
environment_file.write('host={}\n'.format(mysql_ip))
environment_file.close()


# ------------------------ writing to a js file for the front-end -----------------
ip_txt = open("ip.txt", 'w')
ip_txt.write('{}'.format(web_ip))
# ip_js.write('   ip:"{}:5000"\n'.format(web_ip))
# ip_js.write('};\n')
# ip_js.write('\n')
# ip_js.write('export { ip }\n')
ip_txt.close()




# -------------------------------- set up mongodb instance-----------------------
success = False
while(not success):
    try: 
        # start a connection to MongoDB
        c = analytics_functions.theconnector(mongo_ip, key_pair)
        c.sudo('apt-get install gnupg')
        print('----------------------------------------step 1 done---------------------------------------')
        c.run('wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -')
        print('----------------------------------------step 2 done--------------------------------------')
        c.run('echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list')
        print('---------------------------------------step 3 done--------------------------------------')
        c.sudo('apt-get update')
        print('----------------------------------------step 4 done---------------------------------------')
        c.sudo('apt-get install -y mongodb-org')
        print('----------------------------------------step 5 done---------------------------------------')
        c.sudo('service mongod start')
        print('----------------------------------------step 6 done---------------------------------------')

        c.run('wget https://db-project-akmal.s3.amazonaws.com/meta_Kindle_Store.json')
        c.run('wget https://db-project-akmal.s3.amazonaws.com/user_log_record.json')
        c.run('wget https://db-project-akmal.s3.amazonaws.com/book_details.json')

        print('----------------------------------------step 8 done---------------------------------------')
        c.run('mongoimport --db {} --collection {} --file meta_Kindle_Store.json --legacy'.format(name_of_db_meta_data,name_of_collection_meta_data))
        c.run('mongoimport --db {} --collection {} --file user_log_record.json --jsonArray --legacy'.format(name_of_db_user_logs,name_of_collection_user_logs))
        c.run('mongoimport --db extra --collection book_details_collection --file book_details.json --jsonArray --legacy')
        print('----------------------------------------step 7 done---------------------------------------')

        c.sudo("sed -i 's/127.0.0.1/0.0.0.0/g' /etc/mongod.conf")
        print('----------------------------------------step 8 done---------------------------------------')
        c.sudo('service mongod restart')
        print('----------------------------------------step 9 done---------------------------------------')
        success = True
        print('---------------------------------------All steps done--------------------------------------')

    except: 
        # in case fail
        print('something went wrong, retrying i a moment')
        time.sleep(10)

# -------------------------------- set up mysql instance -----------------------
# # Prepare the Mqsql instance
success = False

while(not success):
    try: 
        c = analytics_functions.theconnector(mysql_ip, key_pair)
        # c = analytics_functions.theconnector('3.90.190.227', 'try')
        ### Install the mysql
        print('----------------------------------------step 0 done---------------------------------------')
        c.sudo('apt-get -y install mysql-server')
        print('----------------------------------------step 1 done---------------------------------------')
        c.sudo('mysql -e \'update mysql.user set plugin = "mysql_native_password" where user="root"\'')
        print('----------------------------------------step 2 done---------------------------------------')
        c.sudo('mysql -e \'create user "root"@"%" identified by ""\'')
        print('----------------------------------------step 3 done---------------------------------------')
        c.sudo('mysql -e \'grant all privileges on *.* to "root"@"%" with grant option\'')

        # c.sudo('mysql -e \'grant all privileges on reviews.* to \'sqoop\'@\'%\' identified by \'sqoop123\';\'')
        # c.sudo("""'mysql -e 'GRANT ALL PRIVILEGES on reviews.* to 'sqoop'@'%' identified by 'sqoop123';'""")
        c.sudo('mysql -e \'GRANT ALL PRIVILEGES on reviews.* to "sqoop"@"%" identified by "sqoop123";\'')
        print('----------------------------------------step 4 done---------------------------------------')
        c.sudo('mysql -e "flush privileges"')
        print('----------------------------------------step 5 done---------------------------------------')
        c.sudo('service mysql restart')
        print('----------------------------------------step 6 done---------------------------------------')
        c.sudo('sed -i "s/.*bind-address.*/bind-address = 0.0.0.0/" /etc/mysql/mysql.conf.d/mysqld.cnf')
        c.sudo('service mysql restart')
        print('----------------------------------------step 7 done---------------------------------------')
        c.run('mkdir data')
        print('----------------------------------------step 8 done---------------------------------------')
        # c.run('mkdir data') 
        print('----------------------------------------step 9 done---------------------------------------')
        c.run('cd data && wget -c https://db-project-akmal.s3.amazonaws.com/kindle_reviews.csv')
        print('----------------------------------------step 10 done---------------------------------------')
        c.run('cd data && wget -c https://db-project-akmal.s3.amazonaws.com/kindlereviews.sql')
        print('----------------------------------------step 11 done---------------------------------------')
        c.sudo('mysql -e "create database reviews"')
        
        print('----------------------------------------step 12 done---------------------------------------')
        c.run('cd data && mysql -u root -D reviews -e "source kindlereviews.sql"')
        print('----------------------------------------step 13 done---------------------------------------')
        # c.sudo("sed -i 's/bind-address/#bind-address/g' /etc/mysql/mysql.conf.d/mysqld.cnf")
        # c.sudo("sed -ir 's/127.0.0.1/0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf")
        print('----------------------------------------step 14 done---------------------------------------')
        c.sudo('service mysql restart')
        print('----------------------------------------step 15 done---------------------------------------')

        success = True


    except: 
        # in case fail
        print('something went wrong, retrying i a moment')
        time.sleep(10)


# -------------------------------- set up server instance -----------------------
success = False
while(not success):
    try:
        c = analytics_functions.theconnector(web_ip, key_pair)
        c.run('git clone https://github.com/thawalk/db_flask_server.git')
        c.sudo('apt -y install python3-pip')
        c.put('../.env')
        c.run('mv .env ./db_flask_server')
        c.run('pip3 install --upgrade setuptools')
        c.run('pip3 install mysql-connector')
        c.run('pip3 install flask')
        c.run('pip3 install -U python-dotenv')
        c.run('pip3 install pymongo')
        c.run('pip3 install numpy')
        c.run('pip3 install flask-cors')
        c.put('./ip.txt')
        c.run('git clone https://github.com/sesiliafenina/db-project.git')
        c.run('mv ip.txt ./db-project/')
        c.sudo('apt-get -y install nginx')
        c.sudo('mv /home/ubuntu/db_flask_server/default /etc/nginx/sites-available')
        c.sudo('service nginx restart')
        success = True
    except:
        # in case fail
        print('something went wrong, retrying i a moment')
        time.sleep(10)



# print('-----------------------------------these are the ip address of the instances------------------------------')

# print('mongo ip\n', mongo_ip)
# print('mysql ip\n', mysql_ip)
print("------------------------------Front end website below--------------------------")

print('Website is at:')
print(web_ip)
print('Just copy paste this into the browser')
try:
    c = analytics_functions.theconnector(web_ip,key_pair)
    c.run('cd db_flask_server && sudo python3 app.py')

except ValueError:
    pass

print("------------------------Done-------------------------")








