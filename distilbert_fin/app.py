from fastapi import FastAPI
from pydantic import BaseModel
from transformers import pipeline
import uvicorn

# Load model once
classifier = pipeline(
    "text-classification",
    model="mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
)

app = FastAPI()

class InputData(BaseModel):
    texts: list

@app.post("/predict")
def predict(data: InputData):
    results = classifier(data.texts)
    labels = [r["label"] for r in results]
    return {"labels": labels}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
