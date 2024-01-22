
# Exercise 2: Creating Redshift Cluster using the AWS python SDK 
## An example of Infrastructure-as-code


```python
import pandas as pd
import boto3
import json
```

## STEP 0: (Prerequisite) Save the AWS Access key

### 1. Create a new IAM user
IAM service is a global service, meaning newly created IAM users are not restricted to a specific region by default.
- Go to [AWS IAM service](https://console.aws.amazon.com/iam/home#/users) and click on the "**Add user**" button to create a new IAM user in your AWS account. 
- Choose a name of your choice. 
- Select "*Programmatic access*" as the access type. Click Next. 
- Choose the *Attach existing policies directly* tab, and select the "**AdministratorAccess**". Click Next. 
- Skip adding any tags. Click Next. 
- Review and create the user. It will show you a pair of access key ID and secret.
- Take note of the pair of access key ID and secret. This pair is collectively known as **Access key**. 

<center>
<img style="float: center;height:300px;" src="images/AWS_IAM_1.png"><br><br>
Snapshot of a pair of an Access key
</center>

### <font color='red'>2. Save the access key and secret</font>
Edit the file `dwh.cfg` in the same folder as this notebook and save the access key and secret against the following variables:
```bash
KEY= <YOUR_AWS_KEY>
SECRET= <YOUR_AWS_SECRET>
```
    
For example:
```bash
KEY=6JW3ATLQ34PH3AKI
SECRET=wnoBHA+qUBFgwCRHJqgqrLU0i
```

### 3. Troubleshoot
If your keys are not working, such as getting an `InvalidAccessKeyId` error, then you cannot retrieve them again. You have either of the following two options:

1. **Option 1 - Create a new pair of access keys for the existing user**

 - Go to the [IAM dashboard](https://console.aws.amazon.com/iam/home) and view the details of the existing (Admin) user. 

 - Select on the **Security credentials** tab, and click the **Create access key** button. It will generate a new pair of access key ID and secret.

 - Save the new access key ID and secret in your `dwh.cfg` file


<center>
<img style="float: center;height:450px;" src="images/AWS_IAM_2.png"><br><br>
Snapshot of creating a new Access keys for the existing user
</center>

2. **Option 2 - Create a new IAM user with Admin access** - Refer to the instructions at the top. 

# Load DWH Params from a file


```python
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Param</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>DWH_CLUSTER_TYPE</td>
      <td>multi-node</td>
    </tr>
    <tr>
      <th>1</th>
      <td>DWH_NUM_NODES</td>
      <td>4</td>
    </tr>
    <tr>
      <th>2</th>
      <td>DWH_NODE_TYPE</td>
      <td>dc2.large</td>
    </tr>
    <tr>
      <th>3</th>
      <td>DWH_CLUSTER_IDENTIFIER</td>
      <td>dwhCluster</td>
    </tr>
    <tr>
      <th>4</th>
      <td>DWH_DB</td>
      <td>dwh</td>
    </tr>
    <tr>
      <th>5</th>
      <td>DWH_DB_USER</td>
      <td>dwhuser</td>
    </tr>
    <tr>
      <th>6</th>
      <td>DWH_DB_PASSWORD</td>
      <td>Passw0rd</td>
    </tr>
    <tr>
      <th>7</th>
      <td>DWH_PORT</td>
      <td>5439</td>
    </tr>
    <tr>
      <th>8</th>
      <td>DWH_IAM_ROLE_NAME</td>
      <td>dwhRole</td>
    </tr>
  </tbody>
</table>
</div>



## Create clients and resources for IAM, EC2, S3, and Redshift

To interact with EC2 and S3, utilize `boto3.resource`; for IAM and Redshift, use `boto3.client`. If you require additional details on boto3, refer to the [boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html).

**Note**: We create clients and resources in the **us-west-2** region. Choose the same region in your AWS Web Console to see these resources.


```python
import boto3

ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )

iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
```

# Check out the sample data sources on S3


```python
sampleDbBucket =  s3.Bucket("awssampledbuswest2")
for obj in sampleDbBucket.objects.filter(Prefix="ssbgz"):
    print(obj)
# for obj in sampleDbBucket.objects.all():
#     print(obj)
```

# STEP 1: IAM ROLE
- Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)


```python
from botocore.exceptions import ClientError

#1.1 Create the role, 
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)
    
    
print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)
```

# STEP 2:  Redshift Cluster

- Create a [RedShift Cluster](https://console.aws.amazon.com/redshiftv2/home)
- For complete arguments to `create_cluster`, see [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.create_cluster)


```python
try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)
```

## 2.1 *Describe* the cluster to see its status
- run this block several times until the cluster status becomes `Available`


```python
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Key</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ClusterIdentifier</td>
      <td>dwhcluster</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NodeType</td>
      <td>dc2.large</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ClusterStatus</td>
      <td>available</td>
    </tr>
    <tr>
      <th>3</th>
      <td>MasterUsername</td>
      <td>dwhuser</td>
    </tr>
    <tr>
      <th>4</th>
      <td>DBName</td>
      <td>dwh</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Endpoint</td>
      <td>{'Address': 'dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com', 'Port': 5439}</td>
    </tr>
    <tr>
      <th>6</th>
      <td>VpcId</td>
      <td>vpc-54d40a2c</td>
    </tr>
    <tr>
      <th>7</th>
      <td>NumberOfNodes</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>



<h2> 2.2 Take note of the cluster <font color='red'> endpoint and role ARN </font> </h2>

<font color='red'>DO NOT RUN THIS unless the cluster status becomes "Available". Make ure you are checking your Amazon Redshift cluster in the **us-west-2** region. </font>



```python
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
```

    DWH_ENDPOINT ::  dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com
    DWH_ROLE_ARN ::  arn:aws:iam::988332130976:role/dwhRole


## STEP 3: Open an incoming  TCP port to access the cluster ednpoint


```python
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
```

    ec2.SecurityGroup(id='sg-d6161da0')
    An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule "peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW" already exists


# STEP 4: Make sure you can connect to the cluster


```python
%load_ext sql
```

    The sql extension is already loaded. To reload it, use:
      %reload_ext sql



```python
conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string
```

    postgresql://dwhuser:Passw0rd@dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com:5439/dwh





    'Connected: dwhuser@dwh'



# STEP 5: Clean up your resources

<b><font color='red'>DO NOT RUN THIS UNLESS YOU ARE SURE <br/> 
    We will be using these resources in the next exercises</span></b>


```python
#### CAREFUL!!
#-- Uncomment & run to delete the created resources
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
#### CAREFUL!!
```




    {'Cluster': {'AllowVersionUpgrade': True,
      'AutomatedSnapshotRetentionPeriod': 1,
      'AvailabilityZone': 'us-west-2b',
      'ClusterCreateTime': datetime.datetime(2019, 2, 16, 6, 21, 30, 630000, tzinfo=tzutc()),
      'ClusterIdentifier': 'dwhcluster',
      'ClusterParameterGroups': [{'ParameterApplyStatus': 'in-sync',
        'ParameterGroupName': 'default.redshift-1.0'}],
      'ClusterSecurityGroups': [],
      'ClusterStatus': 'deleting',
      'ClusterSubnetGroupName': 'default',
      'ClusterVersion': '1.0',
      'DBName': 'dwh',
      'Encrypted': False,
      'Endpoint': {'Address': 'dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com',
       'Port': 5439},
      'EnhancedVpcRouting': False,
      'IamRoles': [{'ApplyStatus': 'in-sync',
        'IamRoleArn': 'arn:aws:iam::988332130976:role/dwhRole'}],
      'MasterUsername': 'dwhuser',
      'NodeType': 'dc2.large',
      'NumberOfNodes': 4,
      'PendingModifiedValues': {},
      'PreferredMaintenanceWindow': 'fri:10:30-fri:11:00',
      'PubliclyAccessible': True,
      'Tags': [],
      'VpcId': 'vpc-54d40a2c',
      'VpcSecurityGroups': []},
     'ResponseMetadata': {'HTTPHeaders': {'content-length': '2041',
       'content-type': 'text/xml',
       'date': 'Sat, 16 Feb 2019 07:13:32 GMT',
       'x-amzn-requestid': '5e58b2d8-31ba-11e9-b19b-0945d449b0a9'},
      'HTTPStatusCode': 200,
      'RequestId': '5e58b2d8-31ba-11e9-b19b-0945d449b0a9',
      'RetryAttempts': 0}}



- run this block several times until the cluster really deleted


```python
myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Key</th>
      <th>Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ClusterIdentifier</td>
      <td>dwhcluster</td>
    </tr>
    <tr>
      <th>1</th>
      <td>NodeType</td>
      <td>dc2.large</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ClusterStatus</td>
      <td>deleting</td>
    </tr>
    <tr>
      <th>3</th>
      <td>MasterUsername</td>
      <td>dwhuser</td>
    </tr>
    <tr>
      <th>4</th>
      <td>DBName</td>
      <td>dwh</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Endpoint</td>
      <td>{'Address': 'dwhcluster.csmamz5zxmle.us-west-2.redshift.amazonaws.com', 'Port': 5439}</td>
    </tr>
    <tr>
      <th>6</th>
      <td>VpcId</td>
      <td>vpc-54d40a2c</td>
    </tr>
    <tr>
      <th>7</th>
      <td>NumberOfNodes</td>
      <td>4</td>
    </tr>
  </tbody>
</table>
</div>




```python
#### CAREFUL!!
#-- Uncomment & run to delete the created resources
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
#### CAREFUL!!
```




    {'ResponseMetadata': {'HTTPHeaders': {'content-length': '200',
       'content-type': 'text/xml',
       'date': 'Sat, 16 Feb 2019 07:13:50 GMT',
       'x-amzn-requestid': '694f8d91-31ba-11e9-9438-d3ce9c613ef8'},
      'HTTPStatusCode': 200,
      'RequestId': '694f8d91-31ba-11e9-9438-d3ce9c613ef8',
      'RetryAttempts': 0}}




```python

```
