from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import logging
# Initialisation de l'application FastAPI
app = FastAPI()
# Configuration du logger pour enregistrer les données du webhook
logging.basicConfig(filename="webhook_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")
# Modèle pour les données envoyées par WooCommerce (facultatif)
class WooCommerceWebhook(BaseModel):
    id: int
    status: str
    total: str
    order_key: str
    created_via: str
    customer_id: int = None
    line_items: list = []
    shipping: dict = None

# Endpoint pour recevoir les webhooks
@app.post("/webhook")
async def receive_webhook(request: Request):
    try:
        # Lecture des données brutes envoyées par WooCommerce
        payload = await request.json()   
        # Enregistrer les données dans un fichier (optionnel)
        logging.info(f"Webhook received: {payload}")

@app.post("/webhook-full")
async def receive_webhook(payload: WooCommerceWebhook):
    logging.info(f"Webhook received: {payload}")
    return {"status": "success", "message": "Webhook processed"}
        # Exemple de traitement de données spécifiques (facultatif)
        if "id" in payload:
            logging.info(f"Processing order ID: {payload['id']}")
        # Réponse avec un code 200 pour indiquer la réussite
        return {"status": "success", "message": "Webhook received and processed"}
    except Exception as e:
        logging.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=400, detail="Invalid payload")

# Route de test
@app.get("/")
async def root():
    return {"message": "WooCommerce Webhook API is running"}
