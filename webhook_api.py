import os
import hmac
import hashlib
import base64
from fastapi import FastAPI, Request, HTTPException
import logging

from confluent_kafka import Producer, Consumer


def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce(topic, config):
  # creates a new producer instance
  producer = Producer(config)

  # produces a sample message
  key = "key"
  value = "value"
  producer.produce(topic, key=key, value=value)
  print(f"Produced message to topic {topic}: key = {key:12} value = {value:12}")

  # send any outstanding or buffered messages to the Kafka broker
  producer.flush()

def consume(topic, config):
  # sets the consumer group ID and offset  
  config["group.id"] = "python-group-1"
  config["auto.offset.reset"] = "earliest"

  # creates a new consumer instance
  consumer = Consumer(config)

  # subscribes to the specified topic
  consumer.subscribe([topic])

  try:
    while True:
      # consumer polls the topic and prints any incoming messages
      msg = consumer.poll(1.0)
      if msg is not None and msg.error() is None:
        key = msg.key().decode("utf-8")
        value = msg.value().decode("utf-8")
        print(f"Consumed message from topic {topic}: key = {key:12} value = {value:12}")
  except KeyboardInterrupt:
    pass
  finally:
    # closes the consumer connection
    consumer.close()

# Initialisation de l'application FastAPI
app = FastAPI()

# Configuration du logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Votre secret WooCommerce
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
if not WEBHOOK_SECRET:
    raise Exception("WEBHOOK_SECRET n'est pas défini dans les variables d'environnement")

@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        # Lire le corps brut de la requête
        body = await request.body()
        
        # Récupérer la signature envoyée par WooCommerce
        signature = request.headers.get("X-WC-Webhook-Signature")

        # Déterminer si la requête contient une signature
        if signature:
            computed_signature = base64.b64encode(
                hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).digest()
            ).decode()

            if hmac.compare_digest(signature, computed_signature):
                logging.info("Requête AVEC signature valide.")
                signature_status = "avec signature valide"
            else:
                logging.error("Requête AVEC signature, mais invalide.")
                signature_status = "avec signature invalide"
                raise HTTPException(status_code=403, detail="Invalid signature")
        else:
            logging.warning("Requête SANS signature.")
            signature_status = "sans signature"

        # Traiter les données du webhook
        payload = await request.json()
        logging.info(f"Payload reçu : {payload}")

        # Exemple de réponse
        return {
            "status": "success",
            "message": f"Webhook processed ({signature_status})",
            "data": payload,
        }
    except Exception as e:
        logging.error(f"Erreur lors du traitement du webhook : {str(e)}")
        raise HTTPException(status_code=400, detail=f"Erreur : {str(e)}")

