#!/usr/bin/env python3
"""
Exporta histórico completo do BTC-USD para CSV local.
Fontes: CoinGecko (primária) e Binance (fallback)
"""

import requests
import pandas as pd
import sys
from datetime import datetime

CSV_FILE = "btc_prices.csv"

def fetch_btc_coingecko():
    """
    Baixa histórico completo do BTC via CoinGecko.
    Retorna DataFrame diário com colunas: date, Open, Close.
    """
    print("📡 Tentando CoinGecko API...")
    
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days": "max",  # Histórico completo
        "interval": "daily"
    }
    headers = {"User-Agent": "BTC-Exporter/1.0"}
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=60)
        r.raise_for_status()
        data = r.json()

        prices = data.get("prices", [])
        if not prices:
            raise ValueError("CoinGecko retornou lista vazia")

        print(f"✅ CoinGecko retornou {len(prices)} pontos de dados")

        # Processa dados
        df = pd.DataFrame(prices, columns=["timestamp", "price"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date

        # Agrupa por dia: primeiro preço = Open, último = Close
        grouped = df.groupby("date")["price"]
        df_daily = pd.DataFrame({
            "date": grouped.first().index,
            "Open": grouped.first().values,
            "Close": grouped.last().values,
        })

        print(f"✅ Processados {len(df_daily)} dias únicos")
        print(f"📅 Período: {df_daily['date'].min()} até {df_daily['date'].max()}")
        
        return df_daily
        
    except Exception as e:
        print(f"❌ CoinGecko falhou: {e}")
        return None

def fetch_btc_binance():
    """
    Fallback: Baixa histórico do BTC via Binance.
    """
    print("📡 Tentando Binance API (fallback)...")
    
    url = "https://api.binance.com/api/v3/klines"
    
    all_data = []
    
    # Binance limita a 1000 velas, então fazemos múltiplas chamadas
    # Começando de 2017-08-17 (lançamento do BTCUSDT na Binance)
    start_time = int(datetime(2017, 8, 17).timestamp() * 1000)
    end_time = int(datetime.now().timestamp() * 1000)
    
    current_start = start_time
    
    try:
        while current_start < end_time:
            params = {
                "symbol": "BTCUSDT",
                "interval": "1d",
                "startTime": current_start,
                "limit": 1000
            }
            
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            
            batch = r.json()
            if not batch:
                break
            
            all_data.extend(batch)
            
            # Próximo batch
            last_timestamp = batch[-1][0]
            current_start = last_timestamp + 86400000  # +1 dia
            
            print(f"  ... baixados {len(all_data)} dias até agora")
            
            if len(batch) < 1000:
                break
        
        if not all_data:
            raise ValueError("Binance retornou vazio")
        
        print(f"✅ Binance retornou {len(all_data)} dias")
        
        # Processa dados da Binance
        # Formato: [timestamp, open, high, low, close, volume, ...]
        rows = []
        for candle in all_data:
            date = pd.to_datetime(candle[0], unit="ms").date()
            rows.append({
                "date": date,
                "Open": float(candle[1]),
                "Close": float(candle[4])
            })
        
        df = pd.DataFrame(rows).drop_duplicates("date")
        print(f"📅 Período: {df['date'].min()} até {df['date'].max()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Binance falhou: {e}")
        return None

def fetch_btc_history():
    """
    Tenta múltiplas fontes em ordem de prioridade.
    """
    # Prioridade 1: CoinGecko
    df = fetch_btc_coingecko()
    if df is not None and not df.empty:
        return df
    
    # Prioridade 2: Binance
    df = fetch_btc_binance()
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
