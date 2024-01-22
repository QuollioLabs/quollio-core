import base64
import json
import logging
import time
from typing import Dict, List

import jwt
import requests  # type: ignore
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

logger = logging.getLogger(__name__)


class QDCExternalAPIClient:
    def __init__(self, base_url: str, client_id: str, client_secret: str):
        self.base_url = base_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.auth_token = self._get_auth_token()
        self.session = self._gen_session()

    def _get_auth_token(self) -> str:
        """
        [NOTE]
        Tried to find a package for oauth0 client credentials flow,
        but any of them contains bugs or lacks of features to handle the token refresh when it's expired
        """
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        url = f"{self.base_url}/oauth2/token"
        creds = f"{self.client_id}:{self.client_secret}"
        encoded_creds = base64.b64encode(creds.encode()).decode()
        headers = {"Authorization": f"Basic {encoded_creds}", "Content-Type": "application/x-www-form-urlencoded"}
        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "scope": "api.quollio.com/beta:admin",
        }
        try:
            res = requests.post(url, headers=headers, data=payload)
            res.raise_for_status()
            return json.loads(res.text).get("access_token")
        except ConnectionError as ce:
            logger.error("Connection Error: {}".format(ce))
            raise
        except HTTPError as he:
            logger.error("HTTP Error: {}".format(he))
            raise
        except Timeout as te:
            logger.error("Timeout Error: {}".format(te))
            raise
        except RequestException as re:
            logger.error("Error: {}".format(re))
            raise

    def _refresh_token_if_expired(self):
        decoded_data = jwt.decode(self.auth_token, options={"verify_signature": False})
        if decoded_data.get("exp") < time.time():
            self.auth_token = self._get_auth_token()

    def _gen_session(self) -> requests.Session:
        retry = requests.adapters.Retry(total=1, backoff_factor=1, status_forcelist=[429, 500, 503, 504])
        session = requests.Session()
        session.mount("http://", requests.adapters.HTTPAdapter(max_retries=retry))
        session.mount("https://", requests.adapters.HTTPAdapter(max_retries=retry))
        return session

    def update_stats_by_id(self, global_id: str, payload: Dict[str, List[str]]) -> int:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        self._refresh_token_if_expired()
        headers = {"content-type": "application/json", "authorization": f"Bearer {self.auth_token}"}
        endpoint = f"{self.base_url}/v2/assets/{global_id}/stats"
        try:
            time.sleep(0.5)
            res = self.session.put(endpoint, headers=headers, json=payload)
            res.raise_for_status()
        except ConnectionError as ce:
            logger.error(f"Connection Error: {ce} global_id: {global_id}.")
        except HTTPError as he:
            logger.error(f"HTTP Error: {he} global_id: {global_id}.")
        except Timeout as te:
            logger.error(f"Timeout Error: {te} global_id: {global_id}.")
        except RequestException as re:
            logger.error(f"Error: {re} global_id: {global_id}.")
        else:
            return res.status_code

    def update_lineage_by_id(self, global_id: str, payload: Dict[str, List[str]]) -> int:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s")
        self._refresh_token_if_expired()
        headers = {"content-type": "application/json", "authorization": f"Bearer {self.auth_token}"}
        endpoint = f"{self.base_url}/v2/lineage/{global_id}"
        try:
            time.sleep(0.5)
            res = self.session.put(endpoint, headers=headers, json=payload)
            res.raise_for_status()
        except ConnectionError as ce:
            logger.error(f"Connection Error: {ce} downstream_global_id: {global_id}.")
        except HTTPError as he:
            logger.error(f"HTTP Error: {he} downstream_global_id: {global_id}.")
        except Timeout as te:
            logger.error(f"Timeout Error: {te} downstream_global_id: {global_id}.")
        except RequestException as re:
            logger.error(f"Error: {re} downstream_global_id: {global_id}.")
        else:
            return res.status_code
