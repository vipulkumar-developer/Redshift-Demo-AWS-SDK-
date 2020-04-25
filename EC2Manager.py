import boto3
import json

class EC2Manager:

    def __init__(self):
        self.ec2 = boto3.client('ec2')

    def getSecurityGroupID(self):
        response = self.ec2.describe_security_groups()
        security_group_id = json.dumps(response['SecurityGroups'][0]['GroupId'])
        security_group_id = security_group_id.replace('"', '')
        return security_group_id

    def applyInboundRules(self, security_group_id):
        data = self.ec2.authorize_security_group_ingress(
        GroupId=security_group_id,
        IpPermissions=[
            {'IpProtocol': 'tcp',
             'FromPort': 5439,
             'ToPort': 5439,
             'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
        ])
        return data
    