from typing import Any
from urllib import parse

import msgspec

import nautilus_trader
from nautilus_trader.adapters.bybit.http.errors import BybitError
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.secure import SecureString
from nautilus_trader.core.nautilus_pyo3 import HttpClient, HttpMethod, HttpResponse, Quota, hmac_signature


class BybitResponse(msgspec.Struct, frozen=True):
    retCode: int
    retMsg: str
    result: dict[str, Any]
    time: int | None = None
    retExtInfo: dict[str, Any] | None = None


class BybitHttpClient:
    """
    Bybit asynchronous HTTP client supporting optional authentication.
    """

    def __init__(
        self,
        clock: LiveClock,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str = "https://api.bybit.com",
        recv_window_ms: int = 5_000,
        ratelimiter_quotas: list[tuple[str, Quota]] | None = None,
        ratelimiter_default_quota: Quota | None = None,
    ) -> None:
        self._clock: LiveClock = clock
        self._log: Logger = Logger(name=type(self).__name__)

        self._api_key: str | None = api_key
        self._api_secret: SecureString | None = SecureString(api_secret, name="api_secret") if api_secret else None
        self._recv_window_ms: int = recv_window_ms
        self._base_url: str = base_url

        self._headers: dict[str, Any] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
        }
        if api_key:
            self._headers["X-BAPI-API-KEY"] = api_key

        self._client = HttpClient(
            keyed_quotas=ratelimiter_quotas or [],
            default_quota=ratelimiter_default_quota,
        )
        self._decoder_response = msgspec.json.Decoder(BybitResponse)

    @property
    def api_key(self) -> str | None:
        return self._api_key

    async def send_request(
        self,
        http_method: HttpMethod,
        url_path: str,
        payload: dict[str, str] | None = None,
        signature: str | None = None,
        timestamp: str | None = None,
        ratelimiter_keys: list[str] | None = None,
    ) -> bytes:
        if payload and http_method == HttpMethod.GET:
            url_path += "?" + parse.urlencode(payload)
            payload = None

        url = self._base_url + url_path
        headers = self._headers.copy()

        if signature is not None and self._api_key:
            headers.update({
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-SIGN": signature,
                "X-BAPI-RECV-WINDOW": str(self._recv_window_ms),
            })

        response: HttpResponse = await self._client.request(
            http_method,
            url,
            headers,
            msgspec.json.encode(payload) if payload else None,
            ratelimiter_keys,
        )

        body = response.body
        if response.status >= 400:
            try:
                message = msgspec.json.decode(body) if body else None
            except msgspec.DecodeError:
                message = body.decode()
            raise BybitError(code=response.status, message=message)

        bybit_resp: BybitResponse = self._decoder_response.decode(body)
        if bybit_resp.retCode != 0:
            raise BybitError(code=bybit_resp.retCode, message=bybit_resp.retMsg)

        return body

    async def sign_request(
        self,
        http_method: HttpMethod,
        url_path: str,
        payload: dict[str, str] | None = None,
        ratelimiter_keys: list[str] | None = None,
    ) -> Any:
        # Skip signing if no credentials
        if not self._api_key or not self._api_secret:
            return await self.send_request(
                http_method=http_method,
                url_path=url_path,
                payload=payload,
                ratelimiter_keys=ratelimiter_keys,
            )

        if payload is None:
            payload = {}

        if http_method == HttpMethod.GET:
            timestamp, signature = self._sign_get_request(payload)
        else:
            timestamp, signature = self._sign_post_request(payload)

        return await self.send_request(
            http_method=http_method,
            url_path=url_path,
            payload=payload,
            signature=signature,
            timestamp=timestamp,
            ratelimiter_keys=ratelimiter_keys,
        )

    def _sign_post_request(self, payload: dict[str, Any]) -> tuple[str, str]:
        if not self._api_key or not self._api_secret:
            raise RuntimeError("Cannot sign request: missing api_key or api_secret")

        timestamp = str(self._clock.timestamp_ms())
        payload_str = msgspec.json.encode(payload).decode()
        result = timestamp + self._api_key + str(self._recv_window_ms) + payload_str
        signature = hmac_signature(self._api_secret.get_value(), result)
        return timestamp, signature

    def _sign_get_request(self, payload: dict[str, Any]) -> tuple[str, str]:
        if not self._api_key or not self._api_secret:
            raise RuntimeError("Cannot sign request: missing api_key or api_secret")

        timestamp = str(self._clock.timestamp_ms())
        payload_str = parse.urlencode(payload)
        result = timestamp + self._api_key + str(self._recv_window_ms) + payload_str
        signature = hmac_signature(self._api_secret.get_value(), result)
        return timestamp, signature
