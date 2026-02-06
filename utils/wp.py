import requests
from requests.auth import HTTPBasicAuth

class WordPressClient:
    def __init__(self, base_url: str, user: str, app_password: str):
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(user, app_password)

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def get_or_create_category(self, name: str) -> int:
        # busca por nombre
        r = requests.get(
            self._url("/wp-json/wp/v2/categories"),
            params={"search": name, "per_page": 50},
            auth=self.auth,
            timeout=30
        )
        r.raise_for_status()
        cats = r.json() or []
        for c in cats:
            if (c.get("name") or "").strip().lower() == name.strip().lower():
                return int(c["id"])

        # crea
        r2 = requests.post(
            self._url("/wp-json/wp/v2/categories"),
            json={"name": name},
            auth=self.auth,
            timeout=30
        )
        r2.raise_for_status()
        return int(r2.json()["id"])

    def create_post(self, title: str, content_html: str, status: str = "draft", excerpt: str = "", category_id=None):
        payload = {
            "title": title,
            "content": content_html,
            "status": status,
        }
        if excerpt:
            payload["excerpt"] = excerpt
        if category_id:
            payload["categories"] = [int(category_id)]

        r = requests.post(
            self._url("/wp-json/wp/v2/posts"),
            json=payload,
            auth=self.auth,
            timeout=60
        )
        r.raise_for_status()
        return r.json()
