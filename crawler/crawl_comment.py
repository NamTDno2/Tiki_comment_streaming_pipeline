import requests
import json
import os
from kafka import KafkaProducer
import schedule
import time
from datetime import datetime, timezone
import subprocess
import backoff
import logging

# Configure logging for Kafka producer
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('kafka')
logger.setLevel(logging.INFO)

# Kafka configuration
KAFKA_BROKER = "10.128.0.4:9092" # Thay bằng internal IP của máy ảo Kafka/Processor
TOPIC = "tiki-comments"

print(f"Attempting to connect to Kafka Broker at {KAFKA_BROKER}")
producer = None # Initialize producer as None
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=10,
        max_block_ms=180000,
        request_timeout_ms=90000,
        api_version=(0, 10),
    )
    print("Kafka Producer initialized successfully.")
except Exception as e:
    print(f"Error initializing Kafka Producer: {e}")
    pass

# File to store the last comment timestamp for each product
LAST_TIMESTAMPS_FILE = os.path.expanduser("~/tiki-crawler/last_timestamps.json")

# List of category URLs to crawl
CATEGORY_URLS = [
    "https://tiki.vn/dien-gia-dung/c1882",
    "https://tiki.vn/nha-sach-tiki/c8322",
    "https://tiki.vn/nha-cua-doi_song/c1883",
    "https://tiki.vn/dien-thoai-may-tinh-bang/c1789",
    "https://tiki.vn/do-choi-me-be/c2549",
    "https://tiki.vn/thiet-bi-kts-phu-kien-so/c1815",
    "https://tiki.vn/lam-dep-suc-khoe/c1520",
    "https://tiki.vn/thoi-trang-nu/c931",
    "https://tiki.vn/bach-hoa-online/c4384",
    "https://tiki.vn/thoi-trang-nam/c915",
    "https://tiki.vn/dien-tu-dien-lanh/c4221",
    "https://tiki.vn/phu-kien-thoi-trang/c27498",
    "https://tiki.vn/dong-ho-va-trang-suc/c8371"
]

# Function to parse timestamp (ISO 8601 string or Unix timestamp) into datetime object
def parse_timestamp(timestamp_input):
    timestamp_str = str(timestamp_input)
    try:
        if timestamp_str.endswith('Z'):
            timestamp_str = timestamp_str[:-1] + '+00:00'
        return datetime.fromisoformat(timestamp_str).astimezone(timezone.utc)
    except ValueError:
        try:
            timestamp_int = int(timestamp_input)
            if len(timestamp_str) > 10:
                 timestamp_int = timestamp_int // 1000
            return datetime.fromtimestamp(timestamp_int, tz=timezone.utc)
        except (ValueError, TypeError):
            print(f"Warning: Could not parse timestamp '{timestamp_input}'. Using epoch.")
            return datetime.fromtimestamp(0, tz=timezone.utc)

def format_timestamp(dt_obj):
    return dt_obj.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def load_last_timestamps():
    if os.path.exists(LAST_TIMESTAMPS_FILE):
        try:
            with open(LAST_TIMESTAMPS_FILE, "r") as f:
                timestamps_data = json.load(f)
                return {
                    product_id: parse_timestamp(timestamp_str)
                    for product_id, timestamp_str in timestamps_data.items()
                }
        except Exception as e:
            print(f"Warning: Could not load last timestamps from file: {e}. Starting fresh.")
            return {}
    return {}

def save_last_timestamps(timestamps_dict):
    try:
        timestamps_data = {
            product_id: format_timestamp(timestamp_dt)
            for product_id, timestamp_dt in timestamps_dict.items()
        }
        with open(LAST_TIMESTAMPS_FILE, "w") as f:
            json.dump(timestamps_data, f, indent=4)
    except Exception as e:
        print(f"Error saving last timestamps to file: {e}")

# --- Kafka Producer Callbacks ---
def on_send_success(record_metadata):
    """Callback for successful message delivery."""
    # print(f"Message delivered to topic {record_metadata.topic} [partition {record_metadata.partition}] at offset {record_metadata.offset}")
    pass # Keep callbacks but maybe reduce print frequency

def on_send_error(excp):
    """Callback for message delivery failure."""
    print(f"Message delivery failed: {excp}")
# --- End Callbacks ---


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=5)
def get_product_ids_from_category(category_url):
    """Scrape product IDs from the first page of a category."""
    print(f"--- Attempting to get product IDs from category: {category_url} ---")
    try:
        category_id = category_url.split('/c')[-1].split('-')[-1]
        api_url = f"https://tiki.vn/api/v2/products?limit=40&page=1&category={category_id}"
        print(f"Fetching product IDs from API: {api_url}")
        response = requests.get(api_url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        data = response.json().get("data", [])
        product_ids = [product.get("id") for product in data if product.get("id")]
        print(f"Found {len(product_ids)} product IDs for category {category_id}")
        print(f"--- Finished getting product IDs from category: {category_url} ---")
        return product_ids
    except Exception as e:
        print(f"!!! ERROR getting product IDs from category {category_url}: {e}")
        print(f"--- Finished getting product IDs from category: {category_url} with error ---")
        return []

@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=5)
def crawl_comments_for_product(product_id, last_timestamp_dt):
    """Crawls comments for a specific product incrementally and sends to Kafka."""
    global producer # Declare producer as global
    print(f"--- Starting comment crawl for product {product_id} at {datetime.now()} ---")
    latest_timestamp_dt = last_timestamp_dt
    comments_found_in_this_crawl = 0
    new_comments_count = 0 # Count new comments found for this product

    # Ensure producer is initialized before attempting to send
    if producer is None:
         try:
             producer = KafkaProducer(
                 bootstrap_servers=[KAFKA_BROKER],
                 value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                 retries=10,
                 max_block_ms=180000,
                 request_timeout_ms=90000,
                 api_version=(0, 10),
             )
             print("Kafka producer initialized within crawl_comments_for_product.")
         except Exception as e:
             print(f"Error initializing Kafka producer within crawl_comments_for_product: {e}")
             producer = None # Ensure producer is None if initialization fails
             # We cannot send messages if producer fails to initialize, log and return
             print(f"Skipping comment sending for product {product_id} due to producer error.")
             return 0, latest_timestamp_dt # Return 0 new comments and original timestamp


    print(f"Starting page iteration for product {product_id}...")
    for page in range(1, 6):
        print(f"Processing page {page} for product {product_id}...")
        url = f"https://tiki.vn/api/v2/reviews?product_id={product_id}&page={page}&limit=20"
        try:
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            data = response.json().get("data", [])

            if not data: # Stop if no more comments on this page
                print(f"No comments found on page {page} for product {product_id}. Stopping pagination.")
                break

            page_has_new_comments = False
            print(f"Processing comments on page {page} for product {product_id}...")
            for comment in data:
                comments_found_in_this_crawl += 1
                timestamp_from_api = comment.get("created_at", "1970-01-01T00:00:00Z")
                current_comment_timestamp_dt = parse_timestamp(timestamp_from_api)

                # Only process comments newer than the last recorded timestamp for this product
                if current_comment_timestamp_dt > last_timestamp_dt:
                    page_has_new_comments = True
                    comment_data = {
                        "product_id": product_id,
                        "comment_id": comment.get("id"),
                        "content": comment.get("content"),
                        "rating": comment.get("rating"),
                        "original_timestamp": timestamp_from_api,
                        "crawl_timestamp": format_timestamp(datetime.now(timezone.utc))
                    }
                    # Only send to Kafka if content is not None or empty string after stripping whitespace
                    if comment_data["content"] is not None and str(comment_data["content"]).strip() != "":
                        # --- SEND TO KAFKA IMMEDIATELY ---
                        if producer is not None:
                            try:
                                # Send message asynchronously with callbacks
                                future = producer.send(TOPIC, comment_data)
                                # future.add_callback(on_send_success) # Optional: Enable for success logs
                                future.add_errback(on_send_error) # Log errors
                                new_comments_count += 1 # Count messages successfully queued
                                # print(f"Queued comment {comment_data.get('comment_id', 'N/A')} for product {product_id} to Kafka")
                            except Exception as e:
                                print(f"Error queuing message for comment {comment_data.get('comment_id', 'N/A')} for product {product_id}: {e}")
                        else:
                             print(f"Producer not initialized. Cannot send comment {comment_data.get('comment_id', 'N/A')} for product {product_id} to Kafka.")
                        # ---------------------------------

                        # Update latest timestamp found in this crawl for this product
                        if current_comment_timestamp_dt > latest_timestamp_dt:
                            latest_timestamp_dt = current_comment_timestamp_dt
                    # else:
                    #      print(f"Skipping comment {comment.get('id')} for product {product_id} due to empty content.")


                # If the last comment on the page is older than or equal to the last_timestamp_dt,
                # and this page didn't have any new comments, we've reached comments we've already processed.
                if data and not page_has_new_comments and parse_timestamp(data[-1].get("created_at", "1970-01-01T00:00:00Z")) <= last_timestamp_dt:
                     print(f"Reached old comments on page {page} for product {product_id}. Stopping pagination.")
                     break

            print(f"Finished processing comments on page {page} for product {product_id}.")


        except requests.exceptions.RequestException as e:
            print(f"!!! ERROR crawling page {page} for product {product_id}: {e}. Retrying...")
            raise # Re-raise the exception for backoff
        except Exception as e:
            print(f"!!! UNHANDLED ERROR processing page {page} for product {product_id}: {e}")
            # Don't re-raise for non-request exceptions, just log and continue

    # Flush producer queue after processing each product
    if producer is not None:
        try:
            producer.flush(timeout=10) # Flush with a shorter timeout after each product
            print(f"Kafka producer queue flushed for product {product_id}.")
        except Exception as e:
            print(f"Error flushing Kafka producer for product {product_id}: {e}")

    print(f"--- Finished comment crawl for product {product_id}. Found {comments_found_in_this_crawl} comments in total, {new_comments_count} new comments sent to Kafka. ---")
    return new_comments_count, latest_timestamp_dt # Return count of new comments sent and the latest timestamp found


def main_crawl_job():
    print("Starting main crawl job at", datetime.now())
    last_timestamps = load_last_timestamps()
    latest_timestamps_in_this_crawl = last_timestamps.copy()

    print("Starting category iteration...")
    for i, category_url in enumerate(CATEGORY_URLS):
        print(f"Processing category {i+1}/{len(CATEGORY_URLS)}: {category_url}")
        try:
            product_ids = get_product_ids_from_category(category_url)
            print(f"Found {len(product_ids)} products in category {i+1}")

            print("Starting product iteration for this category...")
            for j, product_id in enumerate(product_ids):
                print(f"Processing product {j+1}/{len(product_ids)} (ID: {product_id}) in category {i+1}...")
                try:
                    last_timestamp_for_product = last_timestamps.get(str(product_id), datetime.fromtimestamp(0, tz=timezone.utc))

                    # crawl_comments_for_product now sends to Kafka directly
                    new_comments_sent_count, latest_timestamp_for_product = crawl_comments_for_product(
                        product_id, last_timestamp_for_product
                    )

                    # Update the latest timestamp found for this product
                    if latest_timestamp_for_product > last_timestamp_for_product:
                         latest_timestamps_in_this_crawl[str(product_id)] = latest_timestamp_for_product

                    # --- THÊM KHOẢNG DỪNG SAU MỖI SẢN PHẨM ---
                    print(f"Finished processing product {product_id}. Waiting 1 second...")
                    time.sleep(1) # Thêm khoảng dừng 1 giây
                    # ---------------------------------------
                except Exception as e:
                     print(f"!!! UNHANDLED ERROR processing product {product_id}: {e}")
                     # Continue to the next product even if one fails
                     pass

            print(f"Finished product iteration for category {i+1}.")
        except Exception as e:
             print(f"!!! UNHANDLED ERROR processing category {i+1} ({category_url}): {e}")
             # Continue to the next category even if one fails
             pass


    print("Finished category iteration.")

    # --- REMOVE GLOBAL SEND AND FLUSH ---
    # Sending and flushing is now done per product within crawl_comments_for_product
    # print(f"Attempting to send {len(all_new_comments)} new comments to Kafka...")
    # ... (Kafka sending logic removed) ...
    # -----------------------------------

    # Save the updated latest timestamps for all products
    print("Saving updated last timestamps...")
    save_last_timestamps(latest_timestamps_in_this_crawl)
    print("Updated last timestamps for all products.")

    print("Main crawl job finished.")


# Schedule the main crawl job more frequently for a streaming feel
schedule.every(5).minutes.do(main_crawl_job)

print("Running initial main crawl job...")
main_crawl_job()
print("Initial main crawl job finished. Scheduling future jobs.")

# Keep the script running to execute scheduled jobs
while True:
    schedule.run_pending()
    time.sleep(1)
