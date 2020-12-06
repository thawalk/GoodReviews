import os, sys
import subprocess
import shutil
sys.path.append('../')
import analytics_functions
sys.path.append('../hadoop')
import scaling


shutil.copy('../hadoop/{}.pem'.format(scaling.key_pair), './')

# change tfidf_output to testcopy
bash_file = open("get_tfidf.sh", 'w')
bash_file.write('scp -i {}.pem -r ubuntu@{}:tfidf_output ./'.format(scaling.key_pair,scaling.namenode_ip))
bash_file.close()

c = analytics_functions.theconnector(scaling.namenode_ip, scaling.key_pair)

# c.run('cd tfidf_output && ls')

print('now getting the TFIDF (this will take 8-10 mins)')
c.run('python3 tfidf.py')
print('downloading the tfidf, will take awhile')

print('now getting the Pearson Correlation (this will take ~1min and printed to console)')
c.run('export PYSPARK_PYTHON=/usr/bin/python3 && python3 pearson.py')


# #test copying
# c.run('mkdir ./testcopy')
# c.run('cp ./tfidf_output/part-00099-5200a268-b05a-403d-b56d-c9d1b2558fd6-c000.csv ./testcopy/')

# get_csv = os.system('get_tfidf.sh')

c.close()




