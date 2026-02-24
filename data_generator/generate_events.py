import json
import uuid
from datetime import datetime, timezone
import random
import time
import os
import yaml

# ----------------- Configuration -----------------
with open(r'data_generator/config.yaml') as f:
    config = yaml.safe_load(f)

output_dir = config['output_dir'] 
os.makedirs(output_dir, exist_ok=True) 

max_events = config['batching']['max_events_per_file'] 
max_seconds = config['batching']['max_batch_seconds'] 

mode = config['mode']
corruption = config['corruption'][mode]

# ----------------- Helpers -----------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def random_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"

# ----------------- Event Generation -----------------
def generate_user_event():
    return {
        "event_id": random_id("event_u"),
        "event_type": user_events,
        "source": "mobile_app",
        "event_ts": now_iso(),
        "ingest_ts": now_iso(),
        "payload": {
            "user_id": random_id("u"),
            "email": random_id("u") + "@gmail.com"
            "country": random.choice(["IN", "US", "JP", "DE"]),
            "device": random.choice(["ios", "android", "web"])
        }
    }

def generate_order_event():
    return {
        "event_id": random_id("event_o"),
        "event_type": order_events,
        "source": "web",
        "event_ts": now_iso(),
        "ingest_ts": now_iso(),
        "payload": {
            "order_id": random_id("o"),
            "user_id": random_id("u"),
            "amount": round(random.uniform(10, 500), 2),
            "currency": random.choice(["INR", "USD", "YEN", "EUR"]),
            "status": random.choice(["order_created", "order_paid"])
        }
    }

def generate_payment_event():
    return {
        "event_id": random_id("event_p"),
        "event_type": payment_events,
        "source": "mobile_app",
        "event_ts": now_iso(),
        "ingest_ts": now_iso(),
        "payload": {
            "payment_id": random_id("u"),
            "order_id": random_id("u") + "@gmail.com"
            "amount": round(random.uniform(10, 500), 2),
            "currency": random.choice(["INR", "USD", "YEN", "EUR"]),
            "status": random.choice(["payment_failed", "payment_success", "payment_inprogress"])
        }
    }

event_generators = {
    "user_events": generate_user_event,
    "order_events": generate_order_event,
    "payment_events": generate_payment_event
}

def maybe_corrupt_payload(event, domain):
    corrupted = False

    if random.random() < corruption['payload_corruption_probability']:
        corrupted = True
    
        if domain == "user_events":
            event['payload'].pop('user_id', None) 
        
        elif domain == "order_events":
            corrupted_fields = random.choice(["remove_order_id", "negative_amount", "invalid_data_type"])
            if corrupted_fields == "remove_order_id":
                event['payload'].pop('order_id', None)
            elif corrupted_fields == "negative_amount":
                if 'amount' in event['payload']:
                    event['payload']['amount'] = -abs(event['payload']['amount'])
            elif corrupted_fields == "invalid_data_type":
                if 'amount' in event['payload']:
                    event['payload']['amount'] = "invalid_amount"
        
        elif domain == "payment_events":
            event["payload"].pop('payment_id', None)

    return event, corrupted

# ----------------- File Writer -----------------
def write_file(domain, events, payload_corrupt_count):
    file_obj = {
        "file_id": f"{domain}_{int(time.time())}_{uuid.uuid4().hex[:5]}",
        "domain": domain,
        "source_system": "generator",
        "created_at": now_iso(),
        "events": events
    }
    filename = f"{file_obj['file_id']}.json"
    path = os.path.join(output_dir, filename)
    
    data = json.dumps(file_obj, indent=2)

    # Malformed JSON simulation
    malformed = False
    if random.random() < corruption['malformed_json_probability']:
        malformed = True
        data = data[:-10]

    # Truncate file simulation
    truncated = False
    if random.random() < corruption['truncate_file_probability']:
        truncated = True
        data = data[:len(data) // 2]

    # Duplicate file simulation
    duplicated = False
    if random.random() < corruption['duplicate_file_probability']:
        duplicated = True
        duplicate_filename = f"{file_obj['file_id']}_duplicate.json"
        duplicate_path = os.path.join(output_dir, duplicate_filename)
        with open(duplicate_path, 'w') as f:
            f.write(data)
    
    with open(path, 'w') as f:
        f.write(data)
    
    print(f"Wrote file {filename} with {len(events)} events")
    print(f"Payload corrupted events in this batch: {payload_corrupt_count}")
    print(f"File corruption status - Malformed JSON: {malformed}, Truncated: {truncated}, Duplicated: {duplicated}")

# ----------------- Main Loop -----------------
def run():
    buffer = []
    last_flush = time.time()
    payload_corrupt_count = 0
    domain = random.choice(list(event_generators.keys()))

    while True:
        event = event_generators[domain]()
        event, corrupted = maybe_corrupt_payload(event, domain)

        if corrupted:
            payload_corrupt_count += 1

        buffer.append(event)

        now = time.time()
        if len(buffer) >= max_events or (now - last_flush) >= max_seconds:
            write_file(domain, buffer, payload_corrupt_count)
            buffer =[]
            last_flush = now
            payload_corrupt_count = 0
            domain = random.choice(list(event_generators.keys()))
        
        time.sleep(0.01) # Stimulate event arrival

if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        print("Shutting down event generator...")
