import asyncio
import json
from pprint import pprint
import websockets
import re

URI = (
  "wss://jetstream.us-east.bsky.network/xrpc/network.bsky.jetstream.subscribeEvents"
  "?collections=app.bsky.feed.post"
  #"&collections=app.bsky.feed.like"
  "&kinds=commit"
)

def is_nyc_post(record, text):
    """Returns True if the post is genuinely about NYC."""
    
    #TODO: add tier1/tier2 here later (tier1 is words guarenteed to be able NYC, 
    #tier2 is words that are likely to be NYC but could be elsewhere, so you need to verify with other word in post. 
    #want to add queens as a word in regex but can't bcuz too common for other words
    
    NYC_TERMS = [
        "new york", "new york city", "nyc", "manhattan", "brooklyn", 
        "bronx", "staten island", "mta", "omny", 
        "mamdani", "hochul", "nypd", 
    ]
    escaped_terms = [re.escape(term) for term in NYC_TERMS]
    NYC_PATTERN = re.compile(r'\b(' + '|'.join(escaped_terms) + r')\b', re.IGNORECASE)

    # Language Filter: Kill Spanish 'mta' and other foreign noise immediately
    langs = record.get("langs", [])
    if langs and "en" not in langs:
        return False
    return bool (NYC_PATTERN.search(text))

def extract_post_payload(event: dict) -> dict:
    """Extracts and formats all fields required for PostgreSQL ingestion."""
    record = event.get("record", {})
    did = event.get("did", "")
    
    # Depending on the Jetstream client parser, rkey might be root or under commit
    commit = event.get("commit", {})
    rkey = commit.get("rkey") or event.get("rkey", "")
    cid = commit.get("cid") or event.get("cid", "")
    
    at_uri = f"at://{did}/app.bsky.feed.post/{rkey}"
    is_reply = bool(record.get("reply"))
    
    return {
        "uri": at_uri,
        "cid": cid,
        "author_did": did,
        "text": record.get("text", ""),
        "created_at": record.get("createdAt"),
        "is_reply": is_reply,
        "like_count": 0,
        "repost_count": 0,
        "is_hydrated": False
    }

async def fetch_data():
  async with websockets.connect(URI, subprotocols=["xrpc.v1.json"]) as ws:
    print("Connected to Jetstream. Listening for NYC posts...\n")
    ingest = []
    async for frame in ws:
      event = json.loads(frame)["payload"]
      if event.get("operation") != "delete":
        # Ensure record and text exist
        record = event.get("record", {})
        text = record.get("text", "")
        
        # Check for NYC terms
        if is_nyc_post(record, text):
          payload = extract_post_payload(event)  
          ingest.append (payload)
          print ("added")
      if len(ingest) == 6:
         pprint(ingest)

def mock_data():
    print("Mock fetching NYC posts...")
    mock_events = [
        {
            "seq": 1,
            "did": "did:example:123",
            "record": {
                "text": "Exploring the streets of New York City!",
                "createdAt": "2024-06-01T12:00:00Z",
                "langs": ["en"]
            }
        },
        {
            "seq": 2,
            "did": "did:example:456",
            "record": {
                "text": "Just had a great bagel in Brooklyn.",
                "createdAt": "2024-06-01T12:05:00Z",
                "langs": ["en"]
            }
        },
        {
            "seq": 3,
            "did": "did:example:789",
            "record": {
                "text": "Visiting the Eiffel Tower in Paris.",
                "createdAt": "2024-06-01T12:10:00Z",
                "langs": ["en"]
            }
        }
    ]
    return mock_events
    
asyncio.run(fetch_data())