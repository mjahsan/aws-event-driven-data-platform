import json
import uuid
from datetime import datetime, timezone
import random
import time
import os
import yaml

# ----------------- Configuration -----------------
with open('config.yaml') as f:
    config = yaml.safe_load(f)

output_dir = config['output_dir'] 
os.makedirs(output_dir, exist_ok=True) 

max_events = config['batching']['max_events_per_file'] 
max_seconds = config['batching']['max_batch_seconds'] 

corruption = config['corruption'] 

# ----------------- Helpers -----------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def random_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"

# ----------------- Event Generation -----------------
def generate_user_event():
    event_type = random.choice(
        ["user_created", "user_login"]
    )

    payload = {
        "user_id": random_id("u"),
        "country": random.choice(["IN", "US", "UK", "DE"]),
        "device": random.choice(["ios", "android", "web"])
    }
    if event_type == "user_login":
        payload["status"] = random.choice(["login_success", "login_failed"])
    else:
        payload["status"] = random.choice(["user_creation_success", "user_creation_failed"])

    return {
        "event_id": random_id("event_u"),
        "event_type": event_type,
        "source": "mobile_app",
        "event_ts": now_iso(),
        "ingest_ts": now_iso(),
        "payload": payload
    }

def generate_order_event():
    return {
        "event_id": random_id("event_o"),
        "event_type": random.choice(["order_created", "order_paid"]),
        "source": "web",
        "event_ts": now_iso(),
        "ingest_ts": now_iso(),
        "payload": {
            "order_id": random_id("o"),
            "user_id": random_id("u"),
            "amount": round(random.uniform(10, 500), 2),
            "currency": "INR"
        }
    }

def generate_payment_event():
    event_type = random.choice(
        ["payment_initiated", "payment_success", "payment_failed"]
    )

    payload = {
        "payment_id": random_id("p"),
        "order_id": random_id("o"),
    }
    if event_type == "payment_failed":
        payload["failure_reason"] = random.choice(["insufficient_funds", "card_expired", "network_error"])
    else:
        payload["amount"] = round(random.uniform(10, 5000), 2)
        payload["currency"] = "INR"

    return {
        "event_id": random_id("event_p"),
        "event_type": event_type,
        "source": "mobile_app",
        "event_ts": now_iso(),
        "ingest_ts": now_iso(),
        "payload": payload
    }

event_generators = {
    "user_events": generate_user_event(),
    "order_events": generate_order_event(),
    "payment_events": generate_payment_event()
}

# ----------------- File Writer -----------------
def write_file(domain, events):
    file_obj = {
        "file_id": f"{domain}_{int(time.time())}",
        "domain": domain,
        "source_system": "generator",
        "generated_at": now_iso(),
        "events": events
    }
    filename = f"{file_obj['file_id']}.json"
    path = os.path.join(output_dir, filename)

    # Duplicate file simulation
    if random.random() < corruption['duplicate_file_probability']:
        print('Writing duplicate file')
    
    data = json.dumps(file_obj, indent=2)

    # Truncate file simulation
    if random.random() < config['truncate_file_probability']:
        data = data[:len(data) // 2]
    
    with open(path, 'w') as f:
        f.write(data)
    
    print(f"Wrote file {filename} with {len(events)} events")

# ----------------- Main Loop -----------------
def run():
    buffer = []
    last_flush = time.time()
    domain = random.choice(list(event_generators.keys()))

    while True:
        event = event_generators[domain]
        buffer.append(event)

        now = time.time()
        if len(buffer) >= max_events or (now - last_flush) >= max_seconds:
            write_file(domain, buffer)
            buffer =[]
            last_flush = now
            domain = random.choice(list(event_generators.keys()))
        
        time.sleep(0.01) # Stimulate upcoming events

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Shutting down event generator...")