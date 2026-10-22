import json
import os
import random
import time
from datetime import datetime, timezone
from azure.storage.filedatalake import DataLakeServiceClient
from faker import Faker

fake = Faker()

# ==========================================
# CONFIGURATION & AZURE CONNECTION DETAILS
# ==========================================
STORAGE_ACCOUNT_NAME = "stecommerceclickstream"  
STORAGE_ACCOUNT_KEY = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

if not STORAGE_ACCOUNT_KEY:
  raise ValueError(
      "AZURE_STORAGE_ACCOUNT_KEY environment variable is not set!"
  )

CONTAINER_NAME = "raw-clickstream"

# Initialize Azure ADLS Gen2 Client
connection_string = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={STORAGE_ACCOUNT_KEY};EndpointSuffix=core.windows.net"
service_client = DataLakeServiceClient.from_connection_string(connection_string)
file_system_client = service_client.get_file_system_client(CONTAINER_NAME)


def generate_clickstream_record(is_fraud_scenario=False):
  """Generates a single multi-country e-commerce clickstream event record.

  Includes currency variations, product categories, and simulated velocity
  fraud.
  """
  countries = {
      "US": {"currency": "USD", "multiplier": 1.0},
      "IN": {"currency": "INR", "multiplier": 83.0},
      "GB": {"currency": "GBP", "multiplier": 0.79},
      "DE": {"currency": "EUR", "multiplier": 0.92},
  }

  country_code = random.choice(list(countries.keys()))
  currency = countries[country_code]["currency"]
  multiplier = countries[country_code]["multiplier"]

  # Base local price
  base_price = round(random.uniform(10.0, 500.0), 2)
  local_price = round(base_price * multiplier, 2)

  user_id = (
      f"usr_{random.randint(1000, 1050)}"
      if not is_fraud_scenario
      else "usr_fraud_999"
  )
  ip_address = (
      fake.ipv4() if not is_fraud_scenario else "192.168.1.100"
  )  # Static IP for fraud simulation

  record = {
      "event_id": fake.uuid4(),
      "user_id": user_id,
      "session_id": fake.uuid4(),
      "event_timestamp": datetime.now(timezone.utc).isoformat(),
      "country_code": country_code,
      "ip_address": ip_address,
      "device_type": random.choice(["Mobile", "Desktop", "Tablet"]),
      "product_category": random.choice(
          ["Electronics", "Fashion", "Home Appliances", "Books"]
      ),
      "local_currency": currency,
      "local_price": local_price,
      "user_email": fake.email(),  # PII data to be masked later via SHA-256
      "page_url": fake.url(),
  }
  return record


def upload_batch_to_adls(records, batch_filename):
  """Uploads generated JSON batch directly to Azure Data Lake Gen2 (Bronze Layer)."""
  file_client = file_system_client.get_file_client(batch_filename)
  data_as_json = "\n".join([json.dumps(rec) for rec in records])

  file_client.upload_data(
      data_as_json, overwrite=True
  )
  print(
      f"[SUCCESS] Uploaded batch {batch_filename} with {len(records)} records"
      " to Azure Data Lake."
  )


# ==========================================
# MAIN EXECUTION LOOP
# ==========================================
if __name__ == "__main__":
  print("Starting Synthetic Clickstream Generation & Azure Upload...")

  # Generate 5 batches of mock data simulating real-time streams
  for batch_id in range(1, 6):
    batch_records = []

    # Normal user clicks
    for _ in range(20):
      batch_records.append(generate_clickstream_record(is_fraud_scenario=False))

    # Injecting Velocity Fraud Scenario (Same User/IP clicking across different countries within seconds)
    fraud_user = "usr_fraud_999"
    fraud_ip = "203.0.113.45"
    timestamp_1 = datetime.now(timezone.utc).isoformat()

    batch_records.append({
        "event_id": fake.uuid4(),
        "user_id": fraud_user,
        "session_id": fake.uuid4(),
        "event_timestamp": timestamp_1,
        "country_code": "IN",
        "ip_address": fraud_ip,
        "device_type": "Mobile",
        "product_category": "Electronics",
        "local_currency": "INR",
        "local_price": 45000.0,
        "user_email": "hacker_bot@evil.com",
        "page_url": fake.url(),
    })

    # Same user clicking from US 3 seconds later (Velocity Fraud Trigger)
    time.sleep(1)
    timestamp_2 = datetime.now(timezone.utc).isoformat()
    batch_records.append({
        "event_id": fake.uuid4(),
        "user_id": fraud_user,
        "session_id": fake.uuid4(),
        "event_timestamp": timestamp_2,
        "country_code": "US",
        "ip_address": fraud_ip,
        "device_type": "Mobile",
        "product_category": "Electronics",
        "local_currency": "USD",
        "local_price": 600.0,
        "user_email": "hacker_bot@evil.com",
        "page_url": fake.url(),
    })

    # File naming convention for Bronze layer ingestion
    file_name = (
        f"clickstream_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{batch_id}.json"
    )
    upload_batch_to_adls(batch_records, file_name)

    # Wait 2 seconds before generating next batch
    time.sleep(2)

  print(
      "All batches successfully generated and pushed to Azure Data Lake Bronze"
      " Layer!"
  )