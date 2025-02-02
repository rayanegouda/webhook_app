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

# Fonction pour lire la configuration Kafka depuis client.properties
def read_kafka_config():
    config = {}
    try:
        with open("client.properties") as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
    except Exception as e:
        logging.error(f"Erreur lors de la lecture du fichier client.properties: {e}")
        raise Exception("Impossible de charger la configuration Kafka")
    return config

# Charger la configuration Kafka
KAFKA_CONFIG = read_kafka_config()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ecommerce.orders.created")

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
