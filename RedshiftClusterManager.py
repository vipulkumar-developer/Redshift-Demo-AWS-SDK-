import boto3

class RedshiftClusterManager:

    def __init__(self):
        self.client = boto3.client('redshift')

    def createClusterWithoutWaiting(self, clusterID, \
        masterUser, masterPassword, arnRole, DBName, numberOfNodes):

        clusterType = ''
        if numberOfNodes == 1:
            clusterType = 'single-node'
        elif numberOfNodes > 1:
            clusterType = 'multi-node'
        elif numberOfNodes < 1:
            numberOfNodes = 1
            clusterType = 'single-node'

        response = self.client.create_cluster(
            DBName=DBName,
            ClusterIdentifier=clusterID,
            ClusterType=clusterType,
            NodeType='dc2.large',
            MasterUsername=masterUser,
            MasterUserPassword=masterPassword,
            Port=5439,
            ClusterVersion='1.0',
            NumberOfNodes=numberOfNodes,
            PubliclyAccessible=True,
            IamRoles=[
                arnRole,
            ],
        )
        return response

    def createClusterAndWait(self, clusterID, \
        masterUser, masterPassword, arnRole, DBName, numberOfNodes):
        waiter = self.client.get_waiter('cluster_available')
        creationResponse = self.createClusterWithoutWaiting(clusterID, \
        masterUser, masterPassword, arnRole, DBName, numberOfNodes)
        print('Creating cluster started')
        waiter.wait(
            ClusterIdentifier=clusterID,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 30
            }
        )
        print('Creating cluster ended')
        return creationResponse

    def getClusterEvents(self, clusterID):
        response = self.client.describe_events(
            SourceIdentifier=clusterID,
            SourceType='cluster'
        )
        return response

    def describeClusters(self, clusterID):
        return self.client.describe_clusters(ClusterIdentifier=clusterID)

    def deleteClusterWithoutWaiting(self, clusterID):
        return self.client.delete_cluster(
            ClusterIdentifier = clusterID,
            SkipFinalClusterSnapshot=True,
        )

    def deleteClusterAndWait(self, clusterID):
        self.deleteClusterWithoutWaiting(clusterID)
        waiter = self.client.get_waiter('cluster_deleted')
        print('Deleting started')
        waiter.wait(
            ClusterIdentifier=clusterID,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 30
            }
        )
        print('Deleting ended')

    def getCredentials(self, clusterID, DbUser, DbName):
        print('DbUser: ' + DbUser)
        print('DbName: ' + DbName)
        return self.client.get_cluster_credentials(
                DbUser=DbUser,
                DbName=DbName,
                ClusterIdentifier=clusterID,
                AutoCreate=False)

    def getEndpoint(self, clusterID):
        return self.describeClusters(clusterID)['Clusters'][0]['Endpoint']['Address'].replace('"', '')
