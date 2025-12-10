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

'''
class InputData(BaseModel): : On crée une classe nommée InputData qui hérite de BaseModel (de la librairie Pydantic). elle est utilisée pour valider les données.
texts : list : on spécifie que l'entrée doit contenir un champ applé texts qui doit être une liste
'''
class InputData(BaseModel):
    texts: list

@app.post("/predict") # dit a FastAPI : quand quelqu'un fait un POST vers /predict, exécute cette fonction
def predict(data: InputData): #La fonction prend en entrée les données envoyées par l'utilisateur, et grâce à InputData, elle sait que c'est un objet contenant une liste appelée texts.
    results = classifier(data.texts)
    labels = [r["label"] for r in results]
    return {"labels": labels}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
