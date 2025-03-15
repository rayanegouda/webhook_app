import os
import time
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
# Fonction pour lire la configuration Kafka depuis client.properties et remplacer par les variables d'environnement
def read_kafka_config():
    config = {}
    try:
        with open("client.properties") as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith("#"):
                    key, value = line.split("=", 1)
                    value = value.strip()

                    # Si la valeur est un placeholder {{VAR_NAME}}, on remplace par la variable d'environnement
                    if value.startswith("{{") and value.endswith("}}"):
                        env_var = value[2:-2]  # Supprime les accolades {{ }}
                        value = os.getenv(env_var, "")  # Remplace par la variable d'env ou "" si non définie

                    config[key.strip()] = value
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
        producer.produce(topic, key=key, value=value, callback=delivery_report)
        producer.poll(0)
        logging.info(f"Message envoyé à Kafka -> Topic: {topic}, Key: {key}, Value: {value}")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi du message Kafka: {str(e)}")

def produce_to_kafka(topic, value):
    try:
        producer.produce(topic, value=value, callback=delivery_report)
        producer.poll(0)
        logging.info(f"Message envoyé à Kafka -> Topic: {topic}, Value: {value}")
    except Exception as e:
        logging.error(f"Erreur lors de l'envoi du message Kafka: {str(e)}")

def delivery_report(err, msg):
    """ Fonction callback pour afficher si le message est bien envoyé """
    if err is not None:
        logging.error(f"❌ Erreur Kafka: {err}")
    else:
        logging.info(f"✅ Message envoyé à {msg.topic()} (partition {msg.partition()})")


@app.post("/webhook")
async def handle_webhook(request: Request):
    try:
        start_time = time.time()
        print(f"⏳ Webhook reçu à {time.strftime('%H:%M:%S')}")

        # Lire le corps brut de la requête AVEC TIMEOUT
        body = await asyncio.wait_for(request.body(), timeout=5)  # ✅ Timeout max 5 sec

        # Vérifier si le body est vide
        if not body:
            logging.error("🚨 Requête webhook reçue avec un body vide !")
            raise HTTPException(status_code=400, detail="Requête Webhook vide")

        # Vérification du type de requête
        content_type = request.headers.get("Content-Type", "")
        if "application/json" not in content_type:
            logging.error(f"🚨 Type de contenu incorrect : {content_type}")
            raise HTTPException(status_code=400, detail="Type de contenu incorrect, JSON attendu")

        # Récupérer la signature WooCommerce
        signature = request.headers.get("X-WC-Webhook-Signature")
        if signature:
            computed_signature = base64.b64encode(
                hmac.new(WEBHOOK_SECRET.encode(), body, hashlib.sha256).digest()
            ).decode()
            if not hmac.compare_digest(signature, computed_signature):
                logging.error("🚨 Signature WooCommerce invalide.")
                raise HTTPException(status_code=403, detail="Signature incorrecte")
            logging.info("🔑 Signature valide.")

        # Lire le JSON du body
        payload = await asyncio.wait_for(request.json(), timeout=5)  # ✅ Timeout max 5 sec
        logging.info(f"📦 Payload reçu : {payload}")

        # Vérifier que le JSON contient des données
        if not payload:
            logging.error("🚨 Le JSON reçu est vide !")
            raise HTTPException(status_code=400, detail="Le JSON reçu est vide")

        # Envoyer à Kafka
        produce_to_kafka(KAFKA_TOPIC, value=str(payload))
        end_time = time.time()
        print(f"✅ Webhook traité en {end_time - start_time:.2f} secondes")

        return {
            "status": "success",
            "message": "Webhook processed and sent to Kafka",
            "data": payload,
        }

    except asyncio.TimeoutError:
        logging.error("⏳ Timeout lors de la lecture du webhook !")
        raise HTTPException(status_code=408, detail="Timeout lors du traitement du webhook")

    except Exception as e:
        logging.error(f"🚨 Erreur : {str(e)}")
        raise HTTPException(status_code=400, detail=f"Erreur : {str(e)}")
