import json
import random
import uuid
from datetime import datetime, timedelta, timezone
import boto3
from pathlib import Path

# === CONFIG ===
BUCKET_NAME = "studocu-poc-events-nishanth"
REGION = "eu-west-1"
TOTAL_EVENTS = 5000
BAD_ROW_PERCENTAGE = 0.05
DAYS_TO_GENERATE = 3  # spreads events across last 3 days for partition demo
LOCAL_OUTPUT_DIR = Path("./fake_events")

EVENT_TYPES = ["document_view", "document_download", "search", "signup", "login", "quiz_attempt"]
COUNTRIES = ["NL", "DE", "US", "IN", "BR", "ES", "FR", "GB", "IT", "MX"]
DEVICE_TYPES = ["desktop", "mobile", "tablet"]
REFERRERS = ["google", "direct", "facebook", "twitter", "instagram", None]
UNIVERSITIES = ["uni_tilburg", "uni_amsterdam", "uni_utrecht", "uni_delft", "uni_eindhoven"]


def generate_good_event(event_date):
    seconds_offset = random.randint(0, 86400)
    event_timestamp = event_date + timedelta(seconds=seconds_offset)
    return {
        "event_id": f"evt_{uuid.uuid4().hex[:12]}",
        "event_timestamp": event_timestamp.isoformat(),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": f"user_{random.randint(10000, 99999)}",
        "session_id": f"sess_{uuid.uuid4().hex[:8]}",
        "document_id": f"doc_{random.randint(10000, 99999)}",
        "university_id": random.choice(UNIVERSITIES),
        "country_code": random.choice(COUNTRIES),
        "device_type": random.choice(DEVICE_TYPES),
        "page_url": f"/document/{random.randint(10000, 99999)}/summary",
        "referrer": random.choice(REFERRERS),
        "duration_seconds": random.randint(5, 600),
        "is_premium_user": random.random() < 0.2
    }


def generate_bad_event(event_date):
    """Intentionally malformed for validation demo."""
    bad_type = random.choice(["missing_field", "wrong_type", "bad_timestamp", "null_required"])
    event = generate_good_event(event_date)

    if bad_type == "missing_field":
        del event["event_id"]
    elif bad_type == "wrong_type":
        event["duration_seconds"] = "not_a_number"
    elif bad_type == "bad_timestamp":
        event["event_timestamp"] = "this is not a timestamp"
    elif bad_type == "null_required":
        event["user_id"] = None

    return event


def generate_events():
    LOCAL_OUTPUT_DIR.mkdir(exist_ok=True)
    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    events_per_day = TOTAL_EVENTS // DAYS_TO_GENERATE
    all_files = []

    for day_offset in range(DAYS_TO_GENERATE):
        event_date = today - timedelta(days=day_offset)
        partition = event_date.strftime("year=%Y/month=%m/day=%d")

        events = []
        for _ in range(events_per_day):
            if random.random() < BAD_ROW_PERCENTAGE:
                events.append(generate_bad_event(event_date))
            else:
                events.append(generate_good_event(event_date))

        # Write as newline-delimited JSON (standard for Spark)
        local_file = LOCAL_OUTPUT_DIR / f"events_{event_date.strftime('%Y%m%d')}.json"
        with open(local_file, "w") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")

        s3_key = f"bronze/{partition}/events_{event_date.strftime('%Y%m%d')}.json"
        all_files.append((local_file, s3_key))
        print(f"Generated {len(events)} events for {event_date.date()} → {local_file}")

    return all_files


def upload_to_s3(files):
    s3 = boto3.client("s3", region_name=REGION)
    for local_file, s3_key in files:
        s3.upload_file(str(local_file), BUCKET_NAME, s3_key)
        print(f"Uploaded → s3://{BUCKET_NAME}/{s3_key}")


if __name__ == "__main__":
    print("Generating fake Studocu events...")
    files = generate_events()
    print("\nUploading to S3...")
    upload_to_s3(files)
    print("\nDone!")