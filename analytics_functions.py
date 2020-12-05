import os
from fabric import Connection

# -------------------------------------------- File for helper functions ------------------------------------------------

# -------------------------------------------- List Public IPs ------------------------------------------------
def list_ec2_instances(ec2):
    print('Public IPs')
    instances = {}
    res = ec2.describe_instances()
    for r in res['Reservations']:
        for ins in r['Instances']:
            if ins['State']['Name'] == 'running' or ins['State']['Name'] == 'pending':
                instances[ins['InstanceId']] = ins['PublicIpAddress']

    print('Instances {}'.format(instances))
    return instances

# -------------------------------------------- List Private IPs ------------------------------------------------

def list_private_ec2_instances(ec2):
    print('Private IPs')
    instances = {}
    res = ec2.describe_instances()
    for r in res['Reservations']:
        for ins in r['Instances']:
            
            if ins['State']['Name'] == 'running' or ins['State']['Name'] == 'pending':
                instances[ins['InstanceId']] = ins['PrivateIpAddress']
                
    print('Instances {}'.format(instances))
    return instances

#  -------------------------------------------------- create hadoop security group, set it to itself ---------------------------------------------------------
def create_security_group(name, description, ip_permissions, ec2):
    response = ec2.describe_vpcs()
    vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')

    response = ec2.create_security_group(
        GroupName=name, Description=description, VpcId=vpc_id)
    security_group_id = response['GroupId']
    print(security_group_id)
    print('Security Group Created {} in vpc {}'.format(security_group_id, vpc_id))

    data = ec2.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=ip_permissions)
    
    print('Ingress Successfully Set {}'.format(data))
    return security_group_id

#  -------------------------------------------------- create hadoop security group, set it to itself---------------------------------------------------------
def create_hadoop_security_group(name, description, ec2):
    print("create security group")

    response = ec2.describe_vpcs()
    vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')

    response = ec2.create_security_group(
        GroupName=name, Description=description, VpcId=vpc_id)
    security_group_id = response['GroupId']
    print(security_group_id)
    print('Security Group Created {} in vpc {}'.format(security_group_id, vpc_id))

    data = ec2.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[{'IpProtocol': 'tcp',
                        'FromPort': 22,
                        'ToPort': 22,
                        'IpRanges': [{
                            'CidrIp': '0.0.0.0/0',
                            'Description': 'SSH'}]},
                       {'IpProtocol': 'tcp',
                        'FromPort': 0,
                        'ToPort': 65535,
                        'UserIdGroupPairs': [{
                            'Description': 'Hadoop Clusters',
                            'GroupId': security_group_id
                        }]
                        }
                       ]
    )
    print('Ingress Successfully Set {}'.format(data))
    return security_group_id

# -------------------------------------------- Create key pair ------------------------------------------------

def create_key_pair(name, ec2):
    response = ec2.create_key_pair(
        KeyName=name
    )
    key = response['KeyMaterial']
    fil = open('{}.pem'.format(name), "w")
    fil.write(key)
    fil.close()

    print('Key Pair {} Created'.format(name))
    return response


#  -------------------------------- helper function to create instance for hadoop with increased volume size--------------------------
def create_instances_hadoop(ami, max_count, instance_type, key, security_group_id, ec2):
    instances = ec2.run_instances(
        ImageId=ami,
        MinCount=1,
        MaxCount=max_count,
        InstanceType=instance_type,
        KeyName=key,
        SecurityGroupIds=[security_group_id],
        BlockDeviceMappings=[
            {
                "DeviceName": "/dev/sda1",
                "Ebs": {"VolumeSize": 32, "VolumeType": "gp2"}
            }
        ]
    )
    instance_list = []
    # print('------------------------checking inside the create hadoop---------------------')
    # print(instances)
    for i in instances['Instances']:
        instance_list.append(i['InstanceId'])

    print('Instances Created {}'.format(instance_list))
    return instance_list

# ------------------------------------------ create instances helper function --------------------------

def create_instances(ami, max_count, instance_type, key, security_group_id, ec2, instance_name):
    # print('------------before the adding the {} instance--------------'.format(instance_name))
    # list_ec2_instances(ec2)

    instances = ec2.run_instances(
        ImageId=ami,
        MinCount=1,
        MaxCount=max_count,
        InstanceType=instance_type,
        KeyName=key,
        SecurityGroupIds=[security_group_id],

    )
    instance_list = []
    print('---------------After the adding the {} instance---------------'.format(instance_name))
    for i in instances['Instances']:
        instance_list.append(i['InstanceId'])

    print('Instances Created {}'.format(instance_list))
    return instance_list

# -------------------------------------------- get public dns ------------------------------------------------
def get_publicdns(ec2):
    dnslist = {}
    res = ec2.describe_instances()
    for r in res['Reservations']:
        for ins in r['Instances']:
            if ins['State']['Name'] == 'running' or ins['State']['Name'] == 'pending':
                dnslist[ins['InstanceId']] = ins['PublicDnsName']
    print('List of active Instances %s' % dnslist)
    return dnslist

# -------------------------------------------- Connector to connect to ec2s ------------------------------------------------
def theconnector(ip, key):
    c = Connection(
        host=ip,
        user="ubuntu",
        connect_kwargs={
            "key_filename": key + ".pem",
        },
    )
    return c

# ------------------------------------------ replace single term function --------------------------
def replace_single_term(template_file, output_file_name, term_to_replace, new_term):
    filetomod = open(template_file)
    lines = [line for line in filetomod]
    newarray = []
    for line in lines:
        output = line
        if term_to_replace in line:
            output = line.replace(term_to_replace, new_term)
        newarray.append(output)
    xml = open(output_file_name, 'w')

    for newlines in newarray:
        xml.write('{}'.format(newlines))

    xml.close()
    filetomod.close()

    return
