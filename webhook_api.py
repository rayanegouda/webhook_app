from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import logging

# Initialisation de l'application FastAPI
app = FastAPI()

# Configuration du logger pour enregistrer les données du webhook
logging.basicConfig(filename="webhook_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")

# Modèle pour les données envoyées par WooCommerce
class WooCommerceWebhook(BaseModel):
    id: int
    status: str
    total: str
    order_key: str
    created_via: str
    customer_id: int = None
    line_items: list = []
    shipping: dict = None

# Endpoint pour recevoir les webhooks (lecture brute des données)
@app.post("/webhook-raw")
async def receive_webhook_raw(request: Request):
    try:
        # Lecture des données brutes envoyées par WooCommerce
        payload = await request.json()   
        
        # Enregistrer les données dans un fichier
        logging.info(f"Webhook received (raw): {payload}")

        # Exemple de traitement de données spécifiques
        if "id" in payload:
            logging.info(f"Processing order ID: {payload['id']}")

        # Réponse avec un code 200 pour indiquer la réussite
        return {"status": "success", "message": "Webhook received and processed"}
    except Exception as e:
        logging.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid payload")

# Endpoint pour recevoir les webhooks avec un modèle Pydantic
@app.post("/webhook")
async def receive_webhook(payload: WooCommerceWebhook):
    try:
        # Enregistrer les données dans un fichier
        logging.info(f"Webhook received (Pydantic): {payload}")

        # Exemple de traitement
        logging.info(f"Processing order ID: {payload.id}, Status: {payload.status}")

        # Réponse avec un code 200 pour indiquer la réussite
        return {"status": "success", "message": "Webhook processed"}
    except Exception as e:
        logging.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid payload")

# Route de test
@app.get("/")
async def root():
    return {"message": "WooCommerce Webhook API is running"}
