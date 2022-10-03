from odd_collector_azure.domain.plugin import AzurePlugin
from typing import Dict, Any, NamedTuple, Optional
from aiohttp import ClientSession


class RequestArgs(NamedTuple):
    method: str
    url: str
    params: Optional[Dict[Any, Any]] = None
    headers: Optional[Dict[Any, Any]] = None
    payload: Optional[Dict[Any, Any]] = None


class AzureClient:
    def __init__(self, config: AzurePlugin):
        self.__config = config

    async def __get_access_token(self) -> str:
        payload = {
            'grant_type': 'password',
            'scope': 'openid',
            'resource': self.__config.resource,
            'client_id': self.__config.client_id,
            'client_secret': self.__config.client_secret,
            'username': self.__config.username,
            'password': self.__config.password
        }
        async with ClientSession() as session:
            response = await self.fetch_async_response(
                session,
                RequestArgs(
                    method="POST",
                    url="https://login.microsoftonline.com/common/oauth2/token",
                    payload=payload,
                ),
            )
            return response.get("access_token")

    async def build_headers(self) -> Dict[str, str]:
        return {"Authorization": "Bearer " + await self.__get_access_token()}

    @staticmethod
    async def fetch_async_response(
            session, request_args: RequestArgs
    ) -> Dict[Any, Any]:
        async with session.request(
                request_args.method,
                url=request_args.url,
                params=request_args.params,
                headers=request_args.headers,
                data=request_args.payload,
        ) as response:
            return await response.json()
