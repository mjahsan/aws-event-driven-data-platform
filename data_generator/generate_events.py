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

