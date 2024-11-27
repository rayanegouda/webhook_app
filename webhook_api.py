import os
import hmac
import hashlib
import base64
from fastapi import FastAPI, Request, HTTPException
import logging

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

