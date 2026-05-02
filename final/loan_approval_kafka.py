# ──────────────────────────────────────────────────────────────
# Required Libraries
# ──────────────────────────────────────────────────────────────
import json
import joblib
import numpy as np
from confluent_kafka import Consumer, Producer
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import time

# ──────────────────────────────────────────────────────────────
# Constants and Configuration
# ──────────────────────────────────────────────────────────────
BROKER_ADDRESS = 'localhost:9092'
INPUT_TOPIC = 'dss-ml-model-input'
OUTPUT_TOPIC = 'dss-ml-model-output'
OUTPUT_TOPIC_LOGS = 'dss-ml-model-output-logs'
CONSUMER_GROUP_ID = 'loan-group'
MODEL_FILE = 'loan_approval_model.pkl'
SCALER_FILE = 'scaler.pkl'

# ──────────────────────────────────────────────────────────────
# Generate Synthetic Dataset for Loan Approval
# ──────────────────────────────────────────────────────────────
def generate_dataset(n_samples=1000):
    np.random.seed(42)
    income = np.random.normal(60000, 20000, n_samples)  # Income ~ N(60k, 20k)
    employment_status = np.random.choice([0, 1], n_samples, p=[0.3, 0.7])  # 70% employed
    credit_score = np.random.randint(300, 851, n_samples)  # Credit score between 300-850
    loan_amount = np.random.normal(20000, 10000, n_samples)  # Loan amount ~ N(20k, 10k)

    X = np.column_stack((income, employment_status, credit_score, loan_amount))
    y = ((employment_status == 1) & (credit_score > 600) & (loan_amount < income * 0.5)).astype(int)
    return X, y

# ──────────────────────────────────────────────────────────────
# Train Logistic Regression Model
# ──────────────────────────────────────────────────────────────
def train_model():
    X, y = generate_dataset()
    X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)

    model = LogisticRegression(random_state=42)
    model.fit(X_train_scaled, y_train)

    joblib.dump(model, MODEL_FILE)
    joblib.dump(scaler, SCALER_FILE)
    print(f"Model saved as {MODEL_FILE} and scaler as {SCALER_FILE}")
    return model, scaler

# ──────────────────────────────────────────────────────────────
# Load Trained Model and Scaler
# ──────────────────────────────────────────────────────────────
try:
    model = joblib.load(MODEL_FILE)
    scaler = joblib.load(SCALER_FILE)
except (OSError, FileNotFoundError):
    print("Model or scaler files not found. Training new model...")
    model, scaler = train_model()

# ──────────────────────────────────────────────────────────────
# Kafka Configuration
# ──────────────────────────────────────────────────────────────
conf_consumer = {
    'bootstrap.servers': BROKER_ADDRESS,
    'group.id': CONSUMER_GROUP_ID,
    'auto.offset.reset': 'earliest'
}

conf_producer = {
    'bootstrap.servers': BROKER_ADDRESS
}

# ──────────────────────────────────────────────────────────────
# Initialize Kafka Consumer and Producer
# ──────────────────────────────────────────────────────────────
consumer = Consumer(conf_consumer)
producer = Producer(conf_producer)

# Subscribe to the input topic
consumer.subscribe([INPUT_TOPIC])

# ──────────────────────────────────────────────────────────────
# Function to Send Prediction to Output Topic
# ──────────────────────────────────────────────────────────────
def send_prediction(approved):
    data = json.dumps({"approved": bool(approved)})
    producer.produce(OUTPUT_TOPIC, value=data.encode('utf-8'))
    producer.flush()

def send_logs(income, employment_status, credit_score, loan_amount, approved):
    data = json.dumps({
        "income": income,
        "employment_status": employment_status,
        "credit_score": credit_score,
        "loan_amount": loan_amount,
        "approved": bool(approved),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    })
    producer.produce(OUTPUT_TOPIC_LOGS, value=data.encode('utf-8'))
    producer.flush()

print("Listening for loan approval prediction...")

# ──────────────────────────────────────────────────────────────
# Main Kafka Polling + ML Prediction Loop
# ──────────────────────────────────────────────────────────────
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        try:
            # Parse the input JSON message
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Received message: {data}")

            # Check for required fields
            required_fields = ["income", "employment_status", "credit_score", "loan_amount"]
            if not all(field in data for field in required_fields):
                print(f"Skipping message: Missing required fields {required_fields}")
                continue

            # Extract and validate fields
            income = data.get("income")
            employment_status = data.get("employment_status")
            credit_score = data.get("credit_score")
            loan_amount = data.get("loan_amount")

            # Convert to float and validate
            try:
                income = float(income)
                credit_score = float(credit_score)
                loan_amount = float(loan_amount)
            except (TypeError, ValueError) as e:
                raise ValueError(f"Invalid numeric value: {e}")

            # Input validation
            if income <= 0 or income > 200000:
                raise ValueError(f"Invalid income: {income}. Must be between 0 and 200,000.")
            if credit_score < 300 or credit_score > 850:
                raise ValueError(f"Invalid credit score: {credit_score}. Must be between 300 and 850.")
            if loan_amount <= 0:
                raise ValueError(f"Invalid loan amount: {loan_amount}. Must be positive.")

            # Validate employment status and map to numeric
            employment_status_lower = employment_status.lower()
            if employment_status_lower not in ['employed', 'unemployed', 'self-employed']:
                raise ValueError(f"Invalid employment status: {employment_status}")
            # Map "Employed" and "Self-Employed" to 1, "Unemployed" to 0
            employment_status_num = 1 if employment_status_lower in ['employed', 'self-employed'] else 0

            # Scale input and make prediction
            input_scaled = scaler.transform([[income, employment_status_num, credit_score, loan_amount]])
            prediction = model.predict(input_scaled)[0]
            approved = int(prediction)
            label = "Approved" if approved else "Not Approved"

            print(f"Received: {income}, {employment_status}, {credit_score}, {loan_amount} → Predicted: {label}")

            # Send prediction result back
            send_prediction(approved)
            send_logs(income, employment_status, credit_score, loan_amount, approved)
        except ValueError as ve:
            print(f"Validation error: {ve}")
        except Exception as e:
            print(f"Error processing message: {e}")

except KeyboardInterrupt:
    print("Shutting down...")

finally:
    consumer.close()

"""
| Feature            | Not Approved (0)            | Approved (1)               |
|--------------------|-----------------------------|----------------------------|
| Income             | Varies, often lower         | Around 60,000 ± 20,000 USD |
| Employment Status  | Often unemployed (0)        | Usually employed (1)       |
| Credit Score       | Often ≤ 600                 | Typically > 600            |
| Loan Amount        | Varies, often > 50% income  | Typically < 50% income     |
"""