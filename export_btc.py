#!/usr/bin/env python3
"""
Exporta histórico completo do BTC-USD para CSV local.
Fonte: Yahoo Finance (via yfinance) - confiável e sem API key
"""

import pandas as pd
import sys
from datetime import datetime, timedelta

CSV_FILE = "btc_prices.csv"

def fetch_btc_yfinance():
    """
    Baixa histórico completo do BTC via Yahoo Finance.
    Retorna DataFrame diário com colunas: date, Open, Close.
    """
    print("📡 Baixando histórico BTC via Yahoo Finance...")
    
    try:
        import yfinance as yf
        
        # BTC-USD está disponível no Yahoo desde ~2014
        # Pega histórico máximo possível
        start_date = "2014-09-17"  # Início do BTC-USD no Yahoo
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"   Período: {start_date} até {end_date}")
        
        # Download com configurações otimizadas
        ticker = yf.Ticker("BTC-USD")
        df = ticker.history(start=start_date, end=end_date, interval="1d")
        
        if df is None or df.empty:
            raise ValueError("Yahoo Finance retornou vazio")
        
        print(f"✅ Yahoo Finance retornou {len(df)} dias")
        
        # Normaliza o formato
        df.index = pd.to_datetime(df.index).date
        df = df.reset_index()
        df.columns = ['date'] + df.columns.tolist()[1:]
        
        # Seleciona apenas Open e Close
        if 'Open' not in df.columns or 'Close' not in df.columns:
            raise ValueError("Colunas Open/Close não encontradas")
        
        result = df[['date', 'Open', 'Close']].copy()
        
        # Remove NaN
        result = result.dropna()
        
        print(f"✅ Processados {len(result)} dias válidos")
        print(f"📅 Período final: {result['date'].min()} até {result['date'].max()}")
        
        return result
        
    except ImportError:
        print("❌ yfinance não instalado. Execute: pip install yfinance")
        return None
    except Exception as e:
        print(f"❌ Yahoo Finance falhou: {e}")
        return None

def fetch_btc_coingecko_free():
    """
    Fallback: Tenta endpoint gratuito do CoinGecko (limitado aos últimos ~365 dias)
    """
    print("📡 Tentando CoinGecko (endpoint gratuito)...")
    
    try:
        import requests
        
        # Endpoint gratuito - últimos 365 dias
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {
            "vs_currency": "usd",
            "days": "365",  # Gratuito suporta até 365 dias
            "interval": "daily"
        }
        headers = {"User-Agent": "BTC-Exporter/1.0"}
        
        r = requests.get(url, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()

        prices = data.get("prices", [])
        if not prices:
            raise ValueError("CoinGecko retornou lista vazia")

        print(f"✅ CoinGecko retornou {len(prices)} pontos")

        # Processa dados
        df = pd.DataFrame(prices, columns=["timestamp", "price"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date

        # Agrupa por dia
        grouped = df.groupby("date")["price"]
        result = pd.DataFrame({
            "date": grouped.first().index,
            "Open": grouped.first().values,
            "Close": grouped.last().values,
        })

        print(f"✅ Processados {len(result)} dias únicos")
        
        return result
        
    except Exception as e:
        print(f"❌ CoinGecko falhou: {e}")
        return None

def fetch_btc_cryptocompare():
    """
    Fallback adicional: CryptoCompare (API gratuita, 2000 dias de histórico)
    """
    print("📡 Tentando CryptoCompare API...")
    
    try:
        import requests
        
        url = "https://min-api.cryptocompare.com/data/v2/histoday"
        
        # CryptoCompare permite até 2000 dias por chamada
        all_data = []
        limit = 2000
        
        # Pega histórico desde 2014
        to_ts = int(datetime.now().timestamp())
        
        params = {
            "fsym": "BTC",
            "tsym": "USD",
            "limit": limit,
            "toTs": to_ts
        }
        
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        
        data = r.json()
        if data.get("Response") != "Success":
            raise ValueError(f"CryptoCompare erro: {data.get('Message')}")
        
        prices = data.get("Data", {}).get("Data", [])
        if not prices:
            raise ValueError("CryptoCompare retornou vazio")
        
        print(f"✅ CryptoCompare retornou {len(prices)} dias")
        
        # Processa dados
        rows = []
        for item in prices:
            date = datetime.fromtimestamp(item["time"]).date()
            rows.append({
                "date": date,
                "Open": float(item["open"]),
                "Close": float(item["close"])
            })
        
        result = pd.DataFrame(rows)
        print(f"📅 Período: {result['date'].min()} até {result['date'].max()}")
        
        return result
        
    except Exception as e:
        print(f"❌ CryptoCompare falhou: {e}")
        return None

def fetch_btc_history():
    """
    Tenta múltiplas fontes em ordem de prioridade.
    """
    # Prioridade 1: Yahoo Finance (mais confiável, histórico completo)
    df = fetch_btc_yfinance()
    if df is not None and not df.empty:
        return df
    
    # Prioridade 2: CryptoCompare (2000 dias de histórico)
    df = fetch_btc_cryptocompare()
    if df is not None and not df.empty:
        return df
    
    # Prioridade 3: CoinGecko free (365 dias apenas)
    df = fetch_btc_coingecko_free()
    if df is not None and not df.empty:
        return df
    
    # Se tudo falhar
    print("❌ ERRO: Todas as fontes falharam")
    return None

def save_csv(df):
    """
    Salva DataFrame em CSV e valida.
    """
    if df is None or df.empty:
        print("❌ ERRO: DataFrame vazio, não salvando CSV")
        sys.exit(1)
    
    try:
        df.to_csv(CSV_FILE, index=False, date_format="%Y-%m-%d")
        print(f"✅ CSV salvo: {CSV_FILE} ({len(df)} linhas)")
        
        # Validação
        test = pd.read_csv(CSV_FILE)
        if len(test) < 100:
            print(f"⚠️ AVISO: CSV tem apenas {len(test)} linhas (esperado >100)")
        
        required_cols = ["date", "Open", "Close"]
        missing = [col for col in required_cols if col not in test.columns]
        if missing:
            print(f"❌ ERRO: Colunas faltando: {missing}")
            sys.exit(1)
        
        print("✅ Validação do CSV passou")
        
    except Exception as e:
        print(f"❌ Erro ao salvar/validar CSV: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("=" * 50)
    print("🚀 Iniciando exportação do histórico BTC")
    print("=" * 50)
    
    df = fetch_btc_history()
    save_csv(df)
    
    print("=" * 50)
    print("✅ Exportação concluída com sucesso!")
    print("=" * 50)
