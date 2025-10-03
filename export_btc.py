#!/usr/bin/env python3
"""
Exporta histórico COMPLETO do BTC-USD desde 2014 para CSV.
Estratégia: Mescla múltiplas fontes para garantir histórico máximo.
"""

import pandas as pd
import sys
from datetime import datetime

CSV_FILE = "btc_prices.csv"

def fetch_btc_yfinance():
    """
    Yahoo Finance: Tenta pegar histórico completo desde 2014.
    """
    print("📡 Tentando Yahoo Finance...")
    
    try:
        import yfinance as yf
        
        # Força download do máximo histórico possível
        # Yahoo tem BTC-USD desde 2014-09-17
        btc = yf.Ticker("BTC-USD")
        
        # Método 1: history com period="max"
        print("   Método 1: period='max'")
        df = btc.history(period="max", interval="1d")
        
        if df.empty:
            # Método 2: download com datas específicas
            print("   Método 2: datas específicas")
            df = yf.download("BTC-USD", start="2010-01-01", end=datetime.now(), progress=False)
        
        if df.empty:
            raise ValueError("Yahoo retornou vazio")
        
        # Normaliza formato
        df = df.reset_index()
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df = df.rename(columns={'Date': 'date'})
        
        result = df[['date', 'Open', 'Close']].copy()
        result = result.dropna()
        
        print(f"✅ Yahoo Finance: {len(result)} dias")
        print(f"   📅 Período: {result['date'].min()} até {result['date'].max()}")
        
        return result
        
    except Exception as e:
        print(f"❌ Yahoo Finance falhou: {e}")
        return None

def fetch_btc_cryptocompare():
    """
    CryptoCompare: Histórico completo em múltiplas chamadas.
    Suporta até 2000 dias por chamada, então fazemos múltiplas.
    """
    print("📡 Tentando CryptoCompare (histórico completo)...")
    
    try:
        import requests
        from datetime import timedelta
        
        url = "https://min-api.cryptocompare.com/data/v2/histoday"
        
        all_data = []
        
        # Pega desde 2010 (antes do BTC existir no mercado moderno)
        end_date = datetime.now()
        current_date = datetime(2010, 1, 1)
        
        batch_num = 0
        while current_date < end_date:
            batch_num += 1
            to_ts = int(min(current_date.timestamp() + (2000 * 86400), end_date.timestamp()))
            
            params = {
                "fsym": "BTC",
                "tsym": "USD",
                "limit": 2000,
                "toTs": to_ts
            }
            
            print(f"   Batch {batch_num}: até {datetime.fromtimestamp(to_ts).date()}")
            
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            
            data = r.json()
            if data.get("Response") != "Success":
                break
            
            prices = data.get("Data", {}).get("Data", [])
            if not prices:
                break
            
            all_data.extend(prices)
            
            # Próximo batch
            last_ts = prices[-1]["time"]
            current_date = datetime.fromtimestamp(last_ts) + timedelta(days=1)
            
            # Evita rate limit
            import time
            time.sleep(0.5)
        
        if not all_data:
            raise ValueError("CryptoCompare retornou vazio")
        
        # Processa dados
        rows = []
        for item in all_data:
            if item["close"] > 0:  # Só dias com preço válido
                date = datetime.fromtimestamp(item["time"]).date()
                rows.append({
                    "date": date,
                    "Open": float(item["open"]),
                    "Close": float(item["close"])
                })
        
        result = pd.DataFrame(rows).drop_duplicates("date").sort_values("date")
        
        print(f"✅ CryptoCompare: {len(result)} dias")
        print(f"   📅 Período: {result['date'].min()} até {result['date'].max()}")
        
        return result
        
    except Exception as e:
        print(f"❌ CryptoCompare falhou: {e}")
        return None

def fetch_btc_coingecko_free():
    """
    CoinGecko: Últimos 365 dias apenas (endpoint gratuito).
    """
    print("📡 Tentando CoinGecko (últimos 365 dias)...")
    
    try:
        import requests
        
        url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
        params = {
            "vs_currency": "usd",
            "days": "365",
            "interval": "daily"
        }
        
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        prices = data.get("prices", [])
        if not prices:
            raise ValueError("CoinGecko retornou vazio")

        df = pd.DataFrame(prices, columns=["timestamp", "price"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date

        grouped = df.groupby("date")["price"]
        result = pd.DataFrame({
            "date": grouped.first().index,
            "Open": grouped.first().values,
            "Close": grouped.last().values,
        })

        print(f"✅ CoinGecko: {len(result)} dias")
        
        return result
        
    except Exception as e:
        print(f"❌ CoinGecko falhou: {e}")
        return None

def merge_sources():
    """
    Mescla dados de múltiplas fontes para maximizar o histórico.
    Prioridade: Yahoo > CryptoCompare > CoinGecko
    """
    print("\n" + "=" * 50)
    print("🔗 MESCLANDO MÚLTIPLAS FONTES")
    print("=" * 50)
    
    sources = []
    
    # Tenta todas as fontes
    df_yahoo = fetch_btc_yfinance()
    if df_yahoo is not None and not df_yahoo.empty:
        sources.append(("Yahoo", df_yahoo))
    
    df_crypto = fetch_btc_cryptocompare()
    if df_crypto is not None and not df_crypto.empty:
        sources.append(("CryptoCompare", df_crypto))
    
    df_gecko = fetch_btc_coingecko_free()
    if df_gecko is not None and not df_gecko.empty:
        sources.append(("CoinGecko", df_gecko))
    
    if not sources:
        print("❌ Nenhuma fonte retornou dados!")
        return None
    
    # Mescla todas as fontes
    print(f"\n📊 Mesclando {len(sources)} fontes...")
    
    all_dfs = [df for _, df in sources]
    merged = pd.concat(all_dfs, ignore_index=True)
    
    # Remove duplicatas, mantendo a primeira ocorrência (prioridade Yahoo)
    merged = merged.drop_duplicates(subset="date", keep="first")
    merged = merged.sort_values("date").reset_index(drop=True)
    
    print(f"\n✅ RESULTADO FINAL:")
    print(f"   Total: {len(merged)} dias únicos")
    print(f"   📅 Período: {merged['date'].min()} até {merged['date'].max()}")
    
    # Mostra contribuição de cada fonte
    for source_name, source_df in sources:
        overlap = len(set(source_df['date'].values) & set(merged['date'].values))
        print(f"   - {source_name}: {overlap} dias na base final")
    
    return merged

def save_csv(df):
    """
    Salva DataFrame em CSV e valida.
    """
    if df is None or df.empty:
        print("❌ ERRO: DataFrame vazio, não salvando CSV")
        sys.exit(1)
    
    try:
        df.to_csv(CSV_FILE, index=False)
        print(f"\n✅ CSV salvo: {CSV_FILE}")
        
        # Validação
        test = pd.read_csv(CSV_FILE)
        
        print(f"✅ Validação:")
        print(f"   - Linhas: {len(test)}")
        print(f"   - Colunas: {test.columns.tolist()}")
        print(f"   - Período: {test['date'].min()} até {test['date'].max()}")
        
        if len(test) < 365:
            print(f"⚠️ AVISO: Menos de 1 ano de dados ({len(test)} dias)")
        
        # Verifica anos disponíveis
        test['year'] = pd.to_datetime(test['date']).dt.year
        years = sorted(test['year'].unique())
        print(f"   - Anos disponíveis: {years[0]} até {years[-1]} ({len(years)} anos)")
        
    except Exception as e:
        print(f"❌ Erro ao salvar/validar CSV: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("=" * 50)
    print("🚀 EXPORTAÇÃO BTC - HISTÓRICO COMPLETO")
    print("=" * 50)
    
    df = merge_sources()
    save_csv(df)
    
    print("\n" + "=" * 50)
    print("✅ Exportação concluída!")
    print("=" * 50)
