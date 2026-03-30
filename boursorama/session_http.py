# boursorama/session_http.py
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import HEADERS


def make_session() -> requests.Session:
    """
    Crée une session requests réutilisable avec retries.
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = make_session()


def fetch_html(url: str, params: dict | None = None) -> str:
    """
    Effectue un GET et renvoie le HTML brut.
    """
    response = SESSION.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.text