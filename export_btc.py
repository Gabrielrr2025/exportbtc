import requests
import pandas as pd

CSV_FILE = "btc_prices.csv"

def fetch_btc_history():
    """
    Baixa histórico completo do BTC em USD via CoinGecko.
    Retorna DataFrame diário com colunas: date, Open, Close.
    """
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=max"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.json()

    prices = data.get("prices", [])  # lista [timestamp, price]
    if not prices:
        raise ValueError("Nenhum dado retornado pela API do CoinGecko")

    df = pd.DataFrame(prices, columns=["timestamp", "price"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date

    # Agrupa por dia para pegar primeiro (Open) e último (Close) preço
    grouped = df.groupby("date")["price"]
    df_daily = pd.DataFrame({
        "Open": grouped.first(),
        "Close": grouped.last(),
    }).reset_index()

    return df_daily

if __name__ == "__main__":
    df = fetch_btc_history()
    df.to_csv(CSV_FILE, index=False, date_format="%Y-%m-%d")
    print(f"✅ BTC histórico salvo em {CSV_FILE} ({len(df)} linhas)")
