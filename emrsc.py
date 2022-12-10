from http import client
from urllib import response
import boto3
import glob
from jproperties import Properties

config = Properties()
with open('/home/yogesh/airflow/dags/Covid_Project_Code/codeconfig.properties', 'rb') as config_file:
    config.load(config_file)

def upload_files(client,file_name,bucket,object_name=None,args=None):
    if object_name == None:
        object_name = file_name
    response = client.upload_file(file_name,bucket,object_name,ExtraArgs=args)

def runemr():
    client = boto3.client('s3')
    bucket = config.get('bucket')[0]
    files = [config.get('file1')[0],config.get('file2')[0],config.get('file3')[0],config.get('file4')[0],config.get('file5')[0],config.get('file6')[0]]
    for file in files:
        var = file.split('/')
        upload_files(client,file,bucket,var[len(var)-1])

    connection = boto3.client(
        'emr'
    )

    cluster_id = connection.run_job_flow(
        Name='covid-project-emr',
        LogUri=config.get('LogUri')[0],
        ReleaseLabel='emr-6.9.0',
        Applications=[
            {
                'Name': 'Spark'
            },
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm1.xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm1.xlarge',
                    'InstanceCount': 2,
                }],
            'Ec2KeyName': config.get('EC2KeyName')[0],
            'KeepJobFlowAliveWhenNoSteps': True,
            'TerminationProtected': False,
            'Ec2SubnetId': config.get('EC2SubnetID')[0],},
        
    Steps=[
        {
            'Name': 'Setup Debugging',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['state-pusher-script']
            }
        },
        {
            'Name': 'setup - copy config file',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['sudo','aws', 's3', 'cp',config.get('s3path1')[0], config.get('destinationpath2')[0]]
            }
        },
        {
            'Name': 'setup - copy jars',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['sudo','aws', 's3', 'cp',config.get('s3path3')[0], config.get('destinationpath')[0]]
            }
        },
        {
            'Name': 'setup - copy jars',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['sudo','aws', 's3', 'cp',config.get('s3path4')[0], config.get('destinationpath')[0]]
            }
        },
        {
            'Name': 'setup - copy jars',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['sudo','aws', 's3', 'cp',config.get('s3path5')[0], config.get('destinationpath')[0]]
            }
        },
        {
            'Name': 'setup - copy files',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['aws', 's3', 'cp',config.get('s3path6')[0], config.get('destinationpath2')[0]]
            }
        },
        {
            'Name': 'setup - copy files',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['aws', 's3', 'cp',config.get('s3path7')[0], config.get('destinationpath2')[0]]
            }
        },
        {
            'Name': 'setup - dependency',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['chmod', '777', config.get('script1')[0]]
            }
        },
        {
            'Name': 'installing dependency',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [config.get('script1')[0]]
            }
        },
        {
            'Name': 'Run Spark',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit','--jars','/usr/share/java/mysql-connector-java-8.0.30.jar,/usr/share/java/aws-java-sdk-bundle-1.11.271.jar,/usr/share/java/hadoop-aws-2.10.1.jar',config.get('script2')[0]]
            }
        }
        ],
        VisibleToAllUsers=True,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole'
    )

    print ('cluster created with the step...', cluster_id['JobFlowId'])