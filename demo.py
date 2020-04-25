from RedshiftClusterManager import RedshiftClusterManager
from S3Manager import S3Manager
from DatabaseManager import DatabaseManager
from IAMRoleManager import IAMRoleManager
from EC2Manager import EC2Manager
import glob
import json
import pprint
import time

def getSelectQueries():
    with open("SQL/SelectQuery1.sql", "r") as file1, \
        open("SQL/SelectQuery2.sql", "r") as file2, \
        open("SQL/SelectQuery3.sql", "r") as file3:
       return file1.read(), file2.read(), file3.read()

def createTables():
    with dbManager as cursor:
        with open('SQL/CreateQuery.sql', 'r') as content_file:
            sqlCreateTables = content_file.read()
            print(sqlCreateTables)
            cursor.execute(sqlCreateTables)

def uploadFilesToS3(bucketName):
    dirPath = "tickitdb\\"
    files = glob.glob(dirPath + "*.txt")
    for filePath in files:
        fileName = filePath.replace(dirPath, '')
        s3Manager.uploadFile(filePath, bucketName, fileName)

def loadDataFromS3ToRedshift(bucketName, iamRoleARN, region):
    uploadFilesToS3(bucketName)
    sqlCopySecondPart = " credentials 'aws_iam_role=" + iamRoleARN + "' delimiter '|' region '" + region + "'; "
    correspondenceTableFile = {
        "users" : "allusers_pipe.txt", 
        "venue" : "venue_pipe.txt", 
        "category" : "category_pipe.txt",
        "date" : "date2008_pipe.txt",
        "event" : "allevents_pipe.txt",
        "listing" : "listings_pipe.txt",
        }

    s3Path = 's3://' + bucketName + '/'
    
    with dbManager as cursor:
        for tableName in correspondenceTableFile.keys():
            fileName = correspondenceTableFile[tableName]
            sqlCopy = "copy " + tableName + " from '" + s3Path + fileName + "' " + sqlCopySecondPart
            print(sqlCopy)
            cursor.execute(sqlCopy)
        sqlCopy = "copy sales from '" + s3Path + "sales_tab.txt" + "' credentials 'aws_iam_role=" + iamRoleARN + "' delimiter '\t' timeformat 'MM/DD/YYYY HH:MI:SS' region '" + region + "';"
        cursor.execute(sqlCopy)

def executeSelectQuery(sqlSelect):
    with dbManager as cursor:
        cursor.execute(sqlSelect)
        return cursor.fetchall()

if __name__ == '__main__':
    start_time = int(round(time.time() * 1000))

    pp = pprint.PrettyPrinter(depth=6)
    redshiftClusterManager = RedshiftClusterManager()
    iamRoleManager = IAMRoleManager()
    s3Manager = S3Manager()
    ec2Manager = EC2Manager()

    bucketName = DBUser = DBName = DBPassword = clusterID = iamRoleName = region =  ''
    with open('C:\\folder\\aws_data.json') as json_file:
        data = json.load(json_file)
        clusterID = data['ClusterID'].lower()
        bucketName = data['BucketName']
        dbUser = data['DBUser']
        dbName = data['DBName']
        dbPassword = data['DBPassword']
        iamRoleName = data['IAMRole']
        region = data['Region']

    arnRole = iamRoleManager.getARNRole(iamRoleName)
    redshiftClusterManager.createClusterAndWait(clusterID, dbUser, dbPassword, arnRole, dbName, 2)
    endpoint = redshiftClusterManager.getEndpoint(clusterID)
    credentials = redshiftClusterManager.getCredentials(clusterID, dbUser, dbName)
    dbManager = DatabaseManager(endpoint, 5439, credentials['DbUser'], credentials['DbPassword'], dbName)

    id = ec2Manager.getSecurityGroupID()
    ec2Manager.applyInboundRules(id)

    s3Manager.createBucket(bucketName, region)
    createTables()
    loadDataFromS3ToRedshift(bucketName, arnRole, region)
    sqlSelect1, sqlSelect2, sqlSelect3 = getSelectQueries()
    pp.pprint(executeSelectQuery(sqlSelect2))

    time_diff = int(round(time.time() * 1000)) - start_time 
    print('Total time: ' + str(time_diff))