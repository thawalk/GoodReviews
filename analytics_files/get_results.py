import os, sys
import subprocess
import shutil
import boto3
sys.path.append('../')
import analytics_functions
sys.path.append('../hadoop')
import scaling



# aws_access_key_id = input('Please key in your AWS access key ID: ')

# aws_secret_access_key = input('Please key in your AWS secret access key: ')

# aws_session_token =input('Please key in your session token: ')

# def aws_session(region_name='us-east-1'):
#     return boto3.session.Session(aws_access_key_id=aws_access_key_id,
#                                 aws_secret_access_key=aws_secret_access_key,
#                                 # aws_session_token=aws_session_token,
#                                 region_name=region_name)

# session = aws_session()
# s3_resource = session.resource('s3')



shutil.copy('../hadoop/{}.pem'.format(scaling.key_pair), './')

# # change tfidf_output to testcopy
# # bash_file = open("get_tfidf.sh", 'w')
# # bash_file.write('scp -i {}.pem -r ubuntu@{}:tfidf_output ./'.format(scaling.key_pair,scaling.namenode_ip))
# # bash_file.close()


# # def upload_file_to_bucket(bucket_name, file_path):
# #     session = aws_session()
# #     s3_resource = session.resource('s3')
# #     file_dir, file_name = os.path.split(file_path)

# #     bucket = s3_resource.Bucket(bucket_name)
# #     bucket.upload_file(
# #       Filename=file_path,

# #       Key=file_name,
# #       ExtraArgs={'ACL': 'public-read'}
# #     )
# #     s3_url = "https://{}.s3.amazonaws.com/{}".format(bucket_name, file_name)
# #     return s3_url






c = analytics_functions.theconnector(scaling.namenode_ip, scaling.key_pair)

print('now getting the TFIDF (this will take 8-10 mins)')
c.run('python3 tfidf.py')
c.run(' yes | sudo apt install zip')
c.sudo('zip -r tfidf_output.zip tfidf_output')

# getting zip file into the analytic_files directory
# it will take around 5 to 10 min

print('downloading the tfidf, will take awhile.....hold on.......')
c.get('tfidf_output.zip')

print('Zip file successfully downloaded')

print('now getting the Pearson Correlation (this will take ~1min and printed to console)')
c.run('export PYSPARK_PYTHON=/usr/bin/python3 && python3 pearson.py')



# # get_csv = os.system('get_tfidf.sh')
# c.sudo('apt-get -y install s3cmd')
# c.run('s3cmd put')
c.close()

# try:
#     s3_resource.meta.client.upload_file('test.py','analytics','test.py',ExtraArgs={'ACL': 'public-read'})

# except Exception as exp:
#     print('exp: ', exp)

# s3_url = upload_file_to_bucket('analyticsurl', 'tfidf_output.zip')

# print('this is the s3 bucket link for tfidf with the excel files inside')
# print(s3_url)




