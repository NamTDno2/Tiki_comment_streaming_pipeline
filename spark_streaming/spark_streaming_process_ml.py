import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, udf, current_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType, FloatType
from pyspark.ml import PipelineModel
from pyspark.ml.linalg import VectorUDT # Needed for working with VectorType columns like 'probability'
import json
from datetime import datetime, timezone
import time

# Import Elasticsearch client libraries
from elasticsearch import Elasticsearch, helpers

# --- Configuration ---
KAFKA_BROKER = "10.128.0.4:9092" # Thay bằng internal IP của máy ảo Kafka/Processor
KAFKA_TOPIC = "tiki-comments"
ELASTICSEARCH_HOST = "10.128.0.3" # Thay bằng internal IP của máy ảo Elasticsearch
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_INDEX = "tiki_comments_streaming"
CHECKPOINT_LOCATION = "/home/nam0837305039/tiki-crawler/checkpoint/tiki_comments_streaming"
# Corrected ML_MODEL_PATH based on your directory structure
ML_MODEL_PATH = "/home/nam0837305039/tiki-crawler/ml_model/spark_full_ml_pipeline_model"
# ---------------------

print("Initializing Spark session...")
# Initialize Spark Session
# Ensure consistent indentation here, typically 4 spaces per level
spark = SparkSession.builder \
    .appName("TikiCommentsStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

print(f"Spark version: {spark.version}")
print("Spark session created.")

# Define the schema for the Kafka message value (JSON string)
schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("comment_id", IntegerType()),
    StructField("content", StringType()),
    StructField("rating", IntegerType()),
    StructField("original_timestamp", StringType()),
    StructField("crawl_timestamp", StringType())
])

print(f"Reading stream from Kafka topic: {KAFKA_TOPIC} on {KAFKA_BROKER}")
# Read data from Kafka
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Select the value field and cast it to String, then parse the JSON
# Also select Kafka's own timestamp for the record
parsed_stream_df = kafka_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_value", "timestamp as kafka_message_timestamp") \
    .select(col("key"), from_json(col("json_value"), schema).alias("data"), col("kafka_message_timestamp")) \
    .select("key", "data.*", "kafka_message_timestamp") # Flatten the struct

print("Schema of parsed_stream_df (from Kafka):")
parsed_stream_df.printSchema()

# Add Kafka timestamp column
df_with_ts = parsed_stream_df.withColumn("kafka_timestamp", col("kafka_message_timestamp").cast(TimestampType()))


# --- ML Model Loading and Application ---
print(f"Loading ML model from {ML_MODEL_PATH}...")
ml_model = None
predictions_df = None # Initialize predictions_df outside the try block

try:
    ml_model = PipelineModel.load(ML_MODEL_PATH)
    print("ML model loaded successfully.")

    # Add a 'content_clean' column as a copy of 'content' for the ML model input
    # This keeps the original 'content' column intact
    df_for_ml_input = df_with_ts.withColumn("content_clean", col("content"))

    # Apply the model transformation
    # The output DataFrame will include original columns + 'content_clean' + ML output columns
    print("Applying ML model transformation...")
    predictions_df = ml_model.transform(df_for_ml_input)
    print("ML model transformation applied. Schema after transformation:")
    predictions_df.printSchema()

    # UDF to extract the probability of the predicted class from the probability vector
    # The probability column from ML model is VectorUDT
    def get_prob_of_predicted_class(prob_vector, prediction_label):
        if prob_vector is None or prediction_label is None:
            return None
        try:
            # prediction_label is usually a double (0.0, 1.0, 2.0, etc.)
            # Ensure prediction_label is not None before converting
            if prediction_label is not None:
                 # Convert double label to int index
                label_index = int(prediction_label)
                # Check if index is within the bounds of the probability vector
                if 0 <= label_index < len(prob_vector):
                    return float(prob_vector[label_index])
                else:
                    # Handle cases where label is out of bounds for the probability vector
                    # print(f"Warning: Prediction label {prediction_label} out of bounds for probability vector length {len(prob_vector)}")
                    return None
            else:
                return None
        except (IndexError, TypeError, ValueError) as e:
            # print(f"Error extracting probability: {e}, vector: {prob_vector}, label: {prediction_label}")
            return None

    # Register the UDF with correct return type (DoubleType for probability)
    get_prob_udf = udf(get_prob_of_predicted_class, DoubleType())

    # Calculate ml_prediction_probability using the UDF
    predictions_df = predictions_df.withColumn(
        "ml_prediction_probability", get_prob_udf(col("probability"), col("prediction"))
    )

    # Calculate ml_predicted_rating based on the ML prediction label
    # Mapping: 0.0 -> 1 star, 1.0 -> 5 stars, others (like 2.0) -> 3 stars
    processed_stream_df = predictions_df \
        .withColumn("ml_prediction_label", col("prediction").cast(DoubleType())) \
        .withColumn("ml_predicted_rating",
                     when(col("prediction") == 0.0, lit(1))
                     .when(col("prediction") == 1.0, lit(5))
                     .otherwise(lit(3).cast(IntegerType())) # Map other labels (like 2.0) to 3 stars
                    )

except Exception as e:
    print(f"Error loading or applying ML model: {e}. Proceeding without applying ML predictions.")
    # If model loading or application fails, create a DataFrame with null columns for ML predictions
    # This ensures the script doesn't crash later due to missing columns
    processed_stream_df = df_with_ts \
        .withColumn("ml_prediction_label", lit(None).cast(DoubleType())) \
        .withColumn("ml_predicted_rating", lit(None).cast(IntegerType())) \
        .withColumn("ml_prediction_probability", lit(None).cast(DoubleType()))


# Select final columns for Elasticsearch
# Include original columns and the ML-derived prediction columns
final_stream_df = processed_stream_df.select(
    col("product_id"),
    col("comment_id"),
    col("content"), # Select the original content column
    col("rating"), # Original rating from Tiki
    col("original_timestamp"),
    col("crawl_timestamp"),
    col("kafka_timestamp"),
    col("ml_prediction_label"), # ML prediction label (DoubleType)
    col("ml_predicted_rating"), # ML predicted rating (IntegerType)
    col("ml_prediction_probability") # ML prediction probability (DoubleType)
    # Other intermediate columns from ML pipeline like 'content_clean', 'features', etc. are dropped by default
)

print("Final DataFrame schema before writing to Elasticsearch:")
final_stream_df.printSchema()

# --- Define the foreachBatch function to write to Elasticsearch ---
def process_batch_to_es(dataframe, batch_id):
    # print(f"Processing batch {batch_id} with {dataframe.count()} rows.") # Keep this print for debugging batches
    if dataframe.count() == 0:
        # print(f"Batch {batch_id} is empty. Skipping Elasticsearch write.") # Keep this print for debugging batches
        return

    # dataframe.persist() # Optional: cache if dataframe is used multiple times below

    try:
        # Collect data to the driver. Be cautious with large batches.
        comments_list = [row.asDict() for row in dataframe.collect()]
        # print(f"Collected {len(comments_list)} rows for batch {batch_id}.") # Keep this print for debugging batches
    except Exception as e:
        print(f"Error collecting DataFrame to list in batch {batch_id}: {e}")
        # dataframe.unpersist() # Unpersist if persisted
        return


    actions = []
    for comment_dict in comments_list:
        # Ensure all values are JSON serializable (e.g., Timestamps convert to ISO strings)
        # Spark's asDict() usually handles this, but be mindful.
        # Convert TimestampType to ISO 8601 string format if ES expects a string
        if 'kafka_timestamp' in comment_dict and isinstance(comment_dict['kafka_timestamp'], datetime):
            comment_dict['kafka_timestamp'] = comment_dict['kafka_timestamp'].isoformat()

        # original_timestamp and crawl_timestamp are already strings from Kafka schema

        doc_id = f"{comment_dict.get('product_id')}_{comment_dict.get('comment_id')}"
        action = {
            "_index": ELASTICSEARCH_INDEX,
            "_id": doc_id,
            "_source": comment_dict
        }
        actions.append(action)

    if not actions:
        # print(f"No actions to perform for Elasticsearch in batch {batch_id}.") # Keep this print for debugging batches
        # dataframe.unpersist() # Unpersist if persisted
        return

    # print(f"Prepared {len(actions)} actions for Elasticsearch bulk indexing in batch {batch_id}.") # Keep this print for debugging batches

    try:
        es_client = Elasticsearch(
            hosts=[{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}],
            # Add authentication if needed
            # basic_auth=("user", "password"),
            # api_key=("id", "api_key"),
            # cloud_id="YOUR_CLOUD_ID",
            # Adjust timeout if needed
            request_timeout=60
        )

        # Check if index exists, create if not (optional, ES can auto-create)
        # For production, pre-creating index with mapping is recommended
        if not es_client.indices.exists(index=ELASTICSEARCH_INDEX):
            print(f"Elasticsearch index '{ELASTICSEARCH_INDEX}' does not exist. Attempting to create.")
            try:
                # Define a simple mapping (adjust as needed)
                mapping = {
                    "properties": {
                        "product_id": {"type": "integer"},
                        "comment_id": {"type": "integer"},
                        "content": {"type": "text", "analyzer": "standard"},
                        "rating": {"type": "integer"},
                        "original_timestamp": {"type": "keyword"}, # Use keyword if not doing date math, or date
                        "crawl_timestamp": {"type": "keyword"},    # Use keyword if not doing date math, or date
                        "kafka_timestamp": {"type": "date"}, # ES can parse ISO strings
                        "ml_prediction_label": {"type": "double"}, # Mapping for ML prediction label (DoubleType)
                        "ml_predicted_rating": {"type": "integer"}, # Mapping for ML predicted rating (IntegerType)
                        "ml_prediction_probability": {"type": "double"} # Mapping for ML prediction probability (DoubleType)
                    }
                }
                # Use ignore=[400] instead of ignore=400 for list of status codes
                es_client.indices.create(index=ELASTICSEARCH_INDEX, mappings=mapping, ignore=[400]) # 'resource_already_exists_exception'
                print(f"Elasticsearch index '{ELASTICSEARCH_INDEX}' creation attempted.")
            except Exception as e_create:
                 print(f"Error creating Elasticsearch index '{ELASTICSEARCH_INDEX}': {e_create}")
                 # Continue, ES might auto-create or it might fail indexing

        # Perform bulk indexing
        # print(f"Performing bulk indexing to Elasticsearch index '{ELASTICSEARCH_INDEX}' for batch {batch_id}...") # Keep this print for debugging batches
        # Use helpers.bulk for efficient indexing
        success_count, errors = helpers.bulk(es_client, actions, chunk_size=500, request_timeout=60, raise_on_error=False) # Increased timeout slightly
        print(f"Bulk indexing for batch {batch_id}: Successfully indexed {success_count} documents.")
        if errors:
            print(f"Encountered {len(errors)} errors during bulk indexing for batch {batch_id}. First 5 errors:")
            for i, error_info in enumerate(errors[:5]):
                print(f"Error {i+1}: {error_info}")
            # Consider logging errors to a file or monitoring system in production

    except Exception as e:
        print(f"!!! UNHANDLED ERROR during Elasticsearch operation for batch {batch_id}: {e}")
    finally:
        pass # dataframe.unpersist() # Unpersist if persisted

# --- Write the stream to Elasticsearch using foreachBatch ---
print(f"Starting to write stream to Elasticsearch index: {ELASTICSEARCH_INDEX} using foreachBatch")
query = final_stream_df.writeStream \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .foreachBatch(process_batch_to_es) \
    .start()

print("writeStream.start() called. Streaming query initiated.")
print(f"Checkpoint location: {CHECKPOINT_LOCATION}")
print("Application will run until manually stopped or an unrecoverable error occurs.")
print("Calling query.awaitTermination()...")

query.awaitTermination()

print("Streaming query terminated.")
