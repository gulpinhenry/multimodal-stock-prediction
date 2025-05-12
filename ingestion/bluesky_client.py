import random
import requests
import configparser


class BlueskyClient:
    def __init__(self, username, password, term):
        self.base_url = "https://bsky.social/xrpc/"
        self.username = username
        self.password = password
        self.session_token = None
        self.term = term

    def authenticate(self):
        url = f"{self.base_url}com.atproto.server.createSession"
        payload = {
            "identifier": self.username,
            "password": self.password
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
        self.session_token = response.json().get("accessJwt")

    def search_posts(self, limit=100, term="Tesla", since=None, until=None):
        if not self.session_token:
            raise ValueError("Client is not authenticated. Please call authenticate() first.")

        url = f"{self.base_url}app.bsky.feed.searchPosts"
        headers = {
            "Authorization": f"Bearer {self.session_token}"
        }
        params = {
            "q": self.term,
            "limit": limit
        }
        if since is not None and until is not None:
            params = {
            "q": term,
            "limit": limit,
            "since": since,
            "until": until
        }
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get("posts", [])