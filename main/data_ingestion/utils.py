import os
import re
import json
from typing import List 

def clean_text(text: str) -> str:
    """
    Nettoie le texte brut :
    - Supprime les sauts de ligne, tabulations, espaces multiples
    - Retire les liens et les caractères spéciaux inutiles
    """
    if not text:
        return ""

    text = re.sub(r'http\S+|www\S+', '', text)
    text = text.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    text = re.sub(r'\s+', ' ', text)
    text = ''.join(ch for ch in text if ch.isprintable())
    return text.strip()


def contains_keywords(text: str, keywords: List[str]) -> bool:
    """
    Vérifie si un texte contient au moins un des mots-clés.
    - Ignore la casse
    - Utilise des correspondances de mots entiers
    """
    if not text or not keywords:
        return False

    text = text.lower()
    return any(
        re.search(rf"\b{re.escape(k)}\b", text)
        for k in keywords
    )

def extract_mentions(text: str) -> List[str]:
    """
    Extrait toutes les mentions d’utilisateurs (@nom) dans un texte.
    """
    if not text:
        return []
    return re.findall(r'@([A-Za-z0-9_]+)', text)


def extract_hashtags(text: str) -> List[str]:
    """
    Extrait les hashtags (#mot) d’un texte.
    """
    if not text:
        return []
    return re.findall(r'#(\w+)', text)

