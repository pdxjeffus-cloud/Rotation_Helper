import os
from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv
from etrade.client import (
    CONSUMER_KEY, CONSUMER_SECRET, BASE,
    REQUEST_TOKEN_URL, ACCESS_TOKEN_URL, AUTHORIZE_URL,
    save_tokens
)

load_dotenv()

def main():
    if not CONSUMER_KEY or not CONSUMER_SECRET:
        raise SystemExit("Missing keys in .env")

    # E*TRADE supports "oob" (out-of-band) verifier/PIN flow for scripts.
    oauth = OAuth1Session(
        CONSUMER_KEY,
        client_secret=CONSUMER_SECRET,
        callback_uri="oob"
    )

    # 1) request token
    fetch_response = oauth.fetch_request_token(REQUEST_TOKEN_URL)
    req_token = fetch_response.get("oauth_token")
    req_secret = fetch_response.get("oauth_token_secret")

    # 2) user authorizes in browser, gets verifier code (PIN)
    auth_url = f"{AUTHORIZE_URL}?key={CONSUMER_KEY}&token={req_token}"
    print("\nOpen this URL in your browser, approve access, and copy the VERIFIER/PIN:\n")
    print(auth_url)
    print("\n")

    verifier = input("Paste verifier/PIN here: ").strip()

    # 3) exchange for access token
    oauth = OAuth1Session(
        CONSUMER_KEY,
        client_secret=CONSUMER_SECRET,
        resource_owner_key=req_token,
        resource_owner_secret=req_secret,
        verifier=verifier
    )
    tokens = oauth.fetch_access_token(ACCESS_TOKEN_URL)
    save_tokens({
        "oauth_token": tokens["oauth_token"],
        "oauth_token_secret": tokens["oauth_token_secret"],
    })

    print("\n✅ Tokens saved to etrade/tokens.json\nNow test quotes with:\n  python3 etrade/test_quote.py\n")

if __name__ == "__main__":
    main()
