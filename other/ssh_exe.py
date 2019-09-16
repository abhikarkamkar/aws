import json
import paramiko

def main():
    key_filename = 'F:\\aws\\s3\\abhishek_key_pair.pem'
    mykey = paramiko.RSAKey.from_private_key_file(key_filename)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect('ec2-13-234-66-109.ap-south-1.compute.amazonaws.com',22,
                    username='hadoop',
                    password='',
                    pkey=mykey)
    except Exception as e:
        print("ssh.connect() raised exception => " + str(e))
        return 1

    stdin, stdout, stderr = ssh.exec_command('export SPARK_HOME=/usr/lib/spark && \
                                             export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH && \
                                             export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$PYTHONPATH && \
                                             python /home/hadoop/sparkdynamo.py')

    results = {}
    for data in stdout:
        results.update(data.asDict())
    ssh.close()


if __name__ == '__main__':
    main()
