import os
import time
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# e.g. https://sandbox.kameapi.cl/oauth/token
API_URL = os.getenv("KAME_API_URL")
CLIENT_ID = os.getenv("KAME_CLIENT_ID")
CLIENT_SECRET = os.getenv("KAME_CLIENT_SECRET")

CACHE_FILE = "token_cache.json"


def get_cached_token():
    """Check if a valid token exists in the cache file."""
    if not os.path.exists(CACHE_FILE):
        return None

    with open(CACHE_FILE, "r") as f:
        data = json.load(f)

    expires_at = data.get("expires_at", 0)
    if time.time() < expires_at:
        # Token still valid
        return data["access_token"]

    return None  # Expired


def save_token(token, expires_in):
    """Save the token and its expiration time."""
    data = {
        "access_token": token,
        "expires_at": time.time() + expires_in - 60  # 1-min buffer
    }
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f)


def request_new_token():
    """Request a new token from Kame API."""
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "audience": "https://api.kameone.cl/api",
        "grant_type": "client_credentials"
    }
    headers = {"Content-Type": "application/json"}

    response = requests.post(API_URL, json=payload, headers=headers)
    response.raise_for_status()
    data = response.json()
    token = data["access_token"]
    expires_in = data.get("expires_in", 86400)  # default 24h
    save_token(token, expires_in)
    return token


def get_token():
    """Return a valid token (cached or freshly requested)."""
    token = get_cached_token()
    if token:
        print("✅ Using cached token")
        return token

    print("🔄 Requesting new token...")
    return request_new_token()


if __name__ == "__main__":
    token = get_token()
    print("Access token:", token)
