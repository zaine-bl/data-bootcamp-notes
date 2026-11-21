import csv
import requests

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "SP.POP.TOTL": "Population, total",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
    "SP.DYN.LE00.IN": "Life expectancy at birth (years)",
}
COLUMNS = [
    "country_code", "country_name", "indicator_code",
    "indicator_label", "year", "value", "load_date",
]
URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"


def fetch_indicator(country, indicator):
    """Retourne toutes les lignes brutes de l'API pour un pays et un indicateur."""
    items = []
    page = 1
    while True:
        resp = requests.get(
            URL.format(country=country, indicator=indicator),
            params={"format": "json", "per_page": 1000, "page": page},
            timeout=30,
        )
        resp.raise_for_status()  # vraies erreurs HTTP (4xx, 5xx)
        payload = resp.json()

        # Point 7 : l'API répond parfois HTTP 200 avec un message d'erreur JSON.
        # Une réponse valide est [métadonnées, [lignes]]. Sinon, c'est une erreur.
        if not isinstance(payload, list) or len(payload) < 2 or payload[1] is None:
            raise ValueError(f"Erreur API World Bank pour {country}/{indicator}: {payload}")

        meta, data = payload
        items.extend(data)
        if page >= int(meta["pages"]):  # pagination
            break
        page += 1
    return items


def extract_to_csv(path, load_date):
    """Écrit le CSV (partie C) et retourne le nombre de lignes."""
    n_rows = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        for country in COUNTRIES:
            for code, label in INDICATORS.items():
                for item in fetch_indicator(country, code):
                    if item["value"] is None:  # année sans donnée : on ignore
                        continue
                    writer.writerow({
                        "country_code": country,
                        "country_name": item["country"]["value"],
                        "indicator_code": code,
                        "indicator_label": label,
                        "year": int(item["date"]),
                        "value": item["value"],
                        "load_date": load_date,
                    })
                    n_rows += 1
    if n_rows == 0:  # sécurité : ne jamais "réussir" avec 0 ligne
        raise ValueError("Aucune ligne extraite")
    return n_rows


if __name__ == "__main__":
    n = extract_to_csv("indicators_test.csv", "2026-11-21")
    print(f"{n} lignes écrites dans indicators_test.csv")