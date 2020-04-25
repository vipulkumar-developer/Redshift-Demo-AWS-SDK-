import boto3

class IAMRoleManager():

    def __init__(self):
        self.client = boto3.client('iam')

    def getARNRole(self, roleName):
        rolesList = self.client.list_roles()
        roles = rolesList['Roles']
        for key in roles:
            if roleName == key['RoleName']:
                return key['Arn']
        return None