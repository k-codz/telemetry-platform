from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import joblib
import logging

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Telemetry ML Anomaly Detector")

# Load the trained model into memory at startup
try:
    model = joblib.load("isolation_forest.joblib")
    logger.info("Successfully loaded Isolation Forest model.")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    raise RuntimeError("Model file not found. Ensure isolation_forest.joblib is present.")

# Define the expected JSON payload format
class PredictRequest(BaseModel):
    total_events: int

# Define the response format
class PredictResponse(BaseModel):
    is_anomaly: bool

@app.post("/predict", response_model=PredictResponse)
def predict_anomaly(req: PredictRequest):
    try:
        # Scikit-Learn models expect a 2D array of features: [[feature1, feature2]]
        features = [[req.total_events]]
        
        # The model returns an array of predictions. [0] gets the first (and only) result.
        # Isolation Forest returns 1 for Normal, and -1 for Anomaly.
        prediction = model.predict(features)[0]
        
        is_anomaly = bool(prediction == -1)
        
        if is_anomaly:
            logger.warning(f"Anomaly detected for event count: {req.total_events}")
            
        return {"is_anomaly": is_anomaly}
        
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        raise HTTPException(status_code=500, detail="Internal ML Error")
        
@app.get("/health")
def health_check():
    return {"status": "healthy", "model": "Isolation Forest"}