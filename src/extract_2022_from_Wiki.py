import requests
import pandas as pd

url = "https://en.wikipedia.org/wiki/2022_FIFA_World_Cup"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)
tables = pd.read_html(response.text)

# Filtrer les tableaux qui contiennent une colonne "Stadium"
match_tables = [t for t in tables if "Stadium" in t.columns or "Venue" in t.columns]

for i, t in enumerate(match_tables):
    print(f"Match table {i}")
    print(t.head())
