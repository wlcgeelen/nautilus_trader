# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import base64
import urllib.parse
from typing import Any

import msgspec

import nautilus_trader
from nautilus_trader.adapters.binance.common.enums import BinanceKeyType
from nautilus_trader.adapters.binance.http.error import BinanceClientError
from nautilus_trader.adapters.binance.http.error import BinanceServerError
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.nautilus_pyo3 import HttpClient, HttpMethod, HttpResponse, Quota
from nautilus_trader.core.nautilus_pyo3 import ed25519_signature, hmac_signature, rsa_signature


class BinanceHttpClient:
    """
    Provides a Binance asynchronous HTTP client.
    Modified to allow unauthenticated use (no API key or secret required).
    """

    def __init__(
        self,
        clock: LiveClock,
        api_key: str | None = None,
        api_secret: str | None = None,
        base_url: str | None = None,
        key_type: BinanceKeyType = BinanceKeyType.HMAC,
        rsa_private_key: str | None = None,
        ed25519_private_key: str | None = None,
        ratelimiter_quotas: list[tuple[str, Quota]] | None = None,
        ratelimiter_default_quota: Quota | None = None,
    ) -> None:
        self._clock: LiveClock = clock
        self._log: Logger = Logger(type(self).__name__)

        # allow None keys
        self._key: str | None = api_key
        self._secret: str | None = api_secret
        self._base_url: str = base_url or "https://fapi.binance.com"
        self._key_type: BinanceKeyType = key_type
        self._rsa_private_key: str | None = rsa_private_key
        self._ed25519_private_key: bytes | None = None

        if ed25519_private_key:
            key_bytes = base64.b64decode(ed25519_private_key)
            self._ed25519_private_key = key_bytes[-32:]

        # Only include X-MBX-APIKEY header if provided
        self._headers: dict[str, Any] = {
            "Content-Type": "application/json",
            "User-Agent": nautilus_trader.NAUTILUS_USER_AGENT,
        }
        if api_key:
            self._headers["X-MBX-APIKEY"] = api_key

        self._client = HttpClient(
            keyed_quotas=ratelimiter_quotas or [],
            default_quota=ratelimiter_default_quota,
        )

    @property
    def base_url(self) -> str:
        """
        Return the base URL being used by the client.

        Returns
        -------
        str

        """
        return self._base_url

    @property
    def api_key(self) -> str | None:
        """
        Return the Binance API key being used by the client.

        Returns
        -------
        str

        """
        return self._key

    @property
    def headers(self):
        """
        Return the headers being used by the client.

        Returns
        -------
        str

        """
        return self._headers

    def _prepare_params(self, params: dict[str, Any]) -> str:
        # Encode a dict into a URL query string
        return urllib.parse.urlencode(params)

    def _get_sign(self, data: str) -> str:
        if not self._secret:
            raise RuntimeError("Cannot sign request: no api_secret provided.")
        match self._key_type:
            case BinanceKeyType.HMAC:
                return hmac_signature(self._secret, data)
            case BinanceKeyType.RSA:
                if not self._rsa_private_key:
                    raise ValueError("`rsa_private_key` was `None`")
                return rsa_signature(self._rsa_private_key, data)
            case BinanceKeyType.ED25519:
                if not self._ed25519_private_key:
                    raise ValueError("`ed25519_private_key` was `None`")
                return ed25519_signature(self._ed25519_private_key, data)
            case _:
                raise ValueError(f"Unsupported key type: '{self._key_type.value}'")

    async def sign_request(
        self,
        http_method: HttpMethod,
        url_path: str,
        payload: dict[str, str] | None = None,
        ratelimiter_keys: list[str] | None = None,
    ) -> Any:
        # if no secret, skip signing completely and just send
        if not self._secret:
            return await self.send_request(
                http_method,
                url_path,
                payload=payload,
                ratelimiter_keys=ratelimiter_keys,
            )

        if payload is None:
            payload = {}
        query_string = self._prepare_params(payload)
        signature = self._get_sign(query_string)
        payload["signature"] = signature
        return await self.send_request(
            http_method,
            url_path,
            payload=payload,
            ratelimiter_keys=ratelimiter_keys,
        )

    async def send_request(
        self,
        http_method: HttpMethod,
        url_path: str,
        payload: dict[str, str] | None = None,
        ratelimiter_keys: list[str] | None = None,
    ) -> bytes:
        if payload:
            url_path += "?" + urllib.parse.urlencode(payload)
            payload = None

        self._log.debug(f"{url_path} {payload}", LogColor.MAGENTA)

        response: HttpResponse = await self._client.request(
            http_method,
            url=self._base_url + url_path,
            headers=self._headers,
            body=msgspec.json.encode(payload) if payload else None,
            keys=ratelimiter_keys,
        )

        body = response.body
        if response.status >= 400:
            try:
                message = msgspec.json.decode(body) if body else None
            except msgspec.DecodeError:
                message = body.decode()
            err_cls = BinanceServerError if response.status >= 500 else BinanceClientError
            raise err_cls(status=response.status, message=message, headers=response.headers)

        return body
