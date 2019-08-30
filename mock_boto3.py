from os import environ

class MockSessionMaker:
    def Session(self):
        return MockSession()

class MockSession:

    def client(self, service_name, region_name):
        return MockClient(service_name, region_name)


class MockClient:
    def __init__(self, service_name, region_name):
        self.service_name = service_name
        self.region_name = region_name

    def get_secret_value(self, SecretId):
        if SecretId == environ['RDS_PW_SECRET_NAME']:
            return {'SecretString': environ['RDS_PASSWORD']}


session = MockSessionMaker()

