from dagster import ConfigurableResource
from mwrogue.esports_client import EsportsClient
from mwrogue.auth_credentials import AuthCredentials

class LeaguepediaResource(ConfigurableResource):
    username: str
    password: str

    def get_client(self) -> EsportsClient:
        credentials = AuthCredentials(
            username=self.username,
            password=self.password
        )
        return EsportsClient("lol", credentials=credentials)