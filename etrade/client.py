import xml.etree.ElementTree as ET
import json
import os
from pathlib import Path
import requests
from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv

load_dotenv()

TOKEN_PATH = Path("etrade/tokens.json")

ENV = os.getenv("ETRADE_ENV", "prod").lower()
BASE = "https://api.etrade.com" if ENV == "prod" else "https://apisb.etrade.com"

REQUEST_TOKEN_URL = f"{BASE}/oauth/request_token"
ACCESS_TOKEN_URL  = f"{BASE}/oauth/access_token"
AUTHORIZE_URL     = "https://us.etrade.com/e/t/etws/authorize"  # same for prod/sandbox

CONSUMER_KEY = os.getenv("ETRADE_CONSUMER_KEY", "").strip()
CONSUMER_SECRET = os.getenv("ETRADE_CONSUMER_SECRET", "").strip()


def _require_keys():
    if not CONSUMER_KEY or not CONSUMER_SECRET:
        raise SystemExit("Missing E*TRADE keys. Set ETRADE_CONSUMER_KEY and ETRADE_CONSUMER_SECRET in .env")


def save_tokens(tokens: dict):
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(json.dumps(tokens, indent=2))


def load_tokens() -> dict | None:
    if TOKEN_PATH.exists():
        return json.loads(TOKEN_PATH.read_text())
    return None


def oauth_session() -> OAuth1Session:
    """Create an OAuth session using saved access tokens (must exist)."""
    _require_keys()
    tok = load_tokens()
    if not tok:
        raise SystemExit("No tokens found. Run: python3 etrade/auth_flow.py")
    return OAuth1Session(
        CONSUMER_KEY,
        client_secret=CONSUMER_SECRET,
        resource_owner_key=tok["oauth_token"],
        resource_owner_secret=tok["oauth_token_secret"],
    )


def get_quote(symbols):
    """Read-only quote call for specific symbols only."""
    if not symbols:
        return {}
    
    sess = oauth_session()
    
    # Clean and uppercase symbols
    sym_list = [s.strip().upper() for s in symbols if s.strip()]
    if not sym_list:
        return {}
    
    sym_str = ','.join(sym_list)
    
    # Correct E*TRADE URL (no .json, symbols in path)
    url = f"{BASE}/v1/market/quote/{sym_str}"
    
    params = {
        "detailFlag": "ALL",
        "overrideSymbolCount": "true"
    }
    

    r = sess.get(url, params=params, headers={'Accept': 'application/json'})
    
    print("DEBUG - Status code:", r.status_code)
    print("DEBUG - Content-Type:", r.headers.get('Content-Type', 'Not set'))
    print("DEBUG - Raw response (first 500 chars):", r.text[:500])
    
    if r.status_code != 200:
        raise Exception(f"API error - Status {r.status_code}: {r.text[:500]}")
    
    return r.json()  # Now safe since we forced JSON

    # Parse XML
    try:
        root = ET.fromstring(r.text)
        
        def xml_to_dict(el):
            result = {}
            if el.text and el.text.strip():
                result[el.tag] = el.text.strip()
            else:
                for child in el:
                    child_dict = xml_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_dict)
                    else:
                        result[child.tag] = child_dict
            return result
        
        xml_data = xml_to_dict(root)
        return {
            'QuoteResponse': {
                'QuoteData': xml_data.get('QuoteData', [])
            }
        }
    except ET.ParseError as e:
        raise Exception(f"XML parse failed: {str(e)} - Raw: {r.text[:200]}")
