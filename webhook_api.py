import os
import hmac
import hashlib
import base64
import logging
from fastapi import FastAPI, Request, HTTPException
from confluent_kafka import Producer

# Initialisation de l'application FastAPI
app = FastAPI()

# Configuration du logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

# Votre secret WooCommerce
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
if not WEBHOOK_SECRET:
    raise Exception("WEBHOOK_SECRET n'est pas défini dans les variables d'environnement")

# Kafka Configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "webhook_topic")
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
}

# Création du producteur Kafka
producer = Producer(KAFKA_CONFIG)

def produce_to_kafka(topic, key, value):
    try:
        producer.produce(topic, key=key, value=value)
        producer.flush()
        logging.info(f"Message envoyé à Kafka -> Topic: {topic}, Key: {key}, Value: {value}")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi du message Kafka: {str(e)}")

@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        # Lire le corps brut de la requête
        body = await request.body()
        
        # Récupérer la signature envoyée par WooCommerce
        signature = request.headers.get("X-WC-Webhook-Signature")

        # Vérifier la signature
        if signature:
            computed_signature = base64.b64encode(
                hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).digest()
            ).decode()

            if not hmac.compare_digest(signature, computed_signature):
                logging.error("Requête AVEC signature, mais invalide.")
                raise HTTPException(status_code=403, detail="Invalid signature")
            logging.info("Requête AVEC signature valide.")
        else:
            logging.warning("Requête SANS signature.")

        # Traiter les données du webhook
        payload = await request.json()
        logging.info(f"Payload reçu : {payload}")
        
        # Envoyer le payload à Kafka
        produce_to_kafka(KAFKA_TOPIC, key="webhook", value=str(payload))
        
        return {
            "status": "success",
            "message": "Webhook processed and sent to Kafka",
            "data": payload,
        }
    except Exception as e:
        logging.error(f"Erreur lors du traitement du webhook : {str(e)}")
        raise HTTPException(status_code=400, detail=f"Erreur : {str(e)}")
