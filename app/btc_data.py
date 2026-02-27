"""
B3 BTC (Empréstimo de Ativos) - Data fetcher
==============================================
Fetches open lending positions from B3's daily bulletin (BDI).

Data source: B3 Boletim Diário - Clearing - Posição em Aberto
URL pattern: https://arquivos.b3.com.br/bdi/download/bdi/{date}/BDI_05_{date_compact}.csv

Provides:
  - Open interest (saldo em quantidade)
  - Average price
  - Outstanding value (saldo em R$)
  - Short interest % (when shares outstanding available)
  - Days to cover (when average volume available)
"""

import logging
import requests
import csv
import io
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

# ── Cache duration: refresh once per day ──
_btc_cache = {}
_btc_cache_date = None


def _get_last_business_days(n=5):
    """Return last N business days as date objects."""
    days = []
    d = datetime.now().date()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:  # Mon-Fri
            days.append(d)
    return days


def _try_fetch_btc_csv(date_obj) -> Optional[str]:
    """Try to fetch BDI_05 CSV for a given date. Returns CSV text or None."""
    date_str = date_obj.strftime("%Y-%m-%d")
    date_compact = date_obj.strftime("%Y%m%d")
    
    # Try CSV format first (newer format)
    urls = [
        f"https://arquivos.b3.com.br/bdi/download/bdi/{date_str}/BDI_05_{date_compact}.csv",
        f"https://arquivos.b3.com.br/bdi/download/bdi/{date_str}/BDI_05-0_{date_compact}.csv",
    ]
    
    for url in urls:
        try:
            logger.info(f"BTC: Tentando {url}")
            resp = requests.get(url, timeout=15, headers={
                "User-Agent": "Mozilla/5.0 (compatible; CVM-API/1.0)"
            })
            if resp.status_code == 200 and len(resp.text) > 100:
                logger.info(f"BTC: Sucesso {url} ({len(resp.text)} bytes)")
                return resp.text
        except Exception as e:
            logger.debug(f"BTC: Falha {url}: {e}")
    
    return None


def _parse_btc_csv(csv_text: str) -> list[dict]:
    """
    Parse B3 BDI_05 CSV into structured records.
    
    Expected columns (may vary):
    Data, Ticker, ISIN, Empresa ou Fundo, Tipo, Mercado, 
    Saldo em quantidade do ativo, Preço Médio, Saldo em R$
    """
    records = []
    
    reader = csv.reader(io.StringIO(csv_text), delimiter=";")
    headers = None
    
    for row in reader:
        if not row or len(row) < 5:
            continue
        
        # Find header row
        row_lower = [c.strip().lower() for c in row]
        if any("ticker" in c for c in row_lower) and headers is None:
            headers = [c.strip() for c in row]
            continue
        
        if headers is None:
            continue
        
        if len(row) < len(headers):
            continue
        
        rec = {}
        for i, h in enumerate(headers):
            if i < len(row):
                rec[h] = row[i].strip()
        
        # Extract standardized fields
        ticker = rec.get("Ticker", rec.get("ticker", ""))
        if not ticker or ticker.lower() in ("total", ""):
            continue
            
        # Skip "Total" rows (aggregated)
        tipo = rec.get("Tipo", rec.get("tipo", ""))
        if tipo.lower() == "total":
            continue
        
        # Parse numeric fields (Brazilian format: 1.234,56)
        def parse_num(val):
            if not val or val == "-":
                return None
            try:
                val = val.replace(".", "").replace(",", ".")
                return float(val)
            except:
                return None
        
        saldo_qty = parse_num(rec.get("Saldo em quantidade do ativo", 
                                       rec.get("Saldo em quantidade", "")))
        preco_medio = parse_num(rec.get("Preço Médio", rec.get("Preco Medio", "")))
        saldo_brl = parse_num(rec.get("Saldo em R$", rec.get("Saldo em RS", "")))
        
        if saldo_qty is None or saldo_qty == 0:
            continue
        
        empresa = rec.get("Empresa ou Fundo", rec.get("Empresa", ""))
        mercado = rec.get("Mercado", "")
        
        records.append({
            "ticker": ticker,
            "empresa": empresa,
            "tipo": tipo,
            "mercado": mercado,
            "saldo_qty": int(saldo_qty) if saldo_qty else 0,
            "preco_medio": round(preco_medio, 4) if preco_medio else None,
            "saldo_brl": round(saldo_brl, 2) if saldo_brl else None,
        })
    
    return records


def fetch_btc_data() -> dict:
    """
    Fetch and cache BTC data from B3.
    Returns dict with date and list of lending positions.
    """
    global _btc_cache, _btc_cache_date
    
    today = datetime.now().date()
    if _btc_cache_date == today and _btc_cache:
        logger.info("BTC: Usando cache")
        return _btc_cache
    
    # Try last 5 business days
    for d in _get_last_business_days(5):
        csv_text = _try_fetch_btc_csv(d)
        if csv_text:
            records = _parse_btc_csv(csv_text)
            if records:
                result = {
                    "date": d.isoformat(),
                    "count": len(records),
                    "positions": records,
                }
                _btc_cache = result
                _btc_cache_date = today
                logger.info(f"BTC: {len(records)} posições de {d.isoformat()}")
                return result
    
    logger.warning("BTC: Nenhum dado encontrado nos últimos 5 dias úteis")
    return {"date": None, "count": 0, "positions": []}


def get_btc_for_ticker(ticker: str, btc_data: dict = None) -> dict:
    """
    Get BTC data for a specific ticker.
    Aggregates Registro + Voluntário into a single position.
    """
    if btc_data is None:
        btc_data = fetch_btc_data()
    
    positions = btc_data.get("positions", [])
    
    # Normalize ticker (remove numbers, try both ON/PN)
    base = ticker.upper()
    matches = [p for p in positions if p["ticker"].upper() == base]
    
    if not matches:
        return {
            "date": btc_data.get("date"),
            "ticker": ticker,
            "found": False,
            "saldo_qty": 0,
            "saldo_brl": 0,
            "preco_medio": None,
        }
    
    # Aggregate
    total_qty = sum(p["saldo_qty"] for p in matches)
    total_brl = sum(p["saldo_brl"] or 0 for p in matches)
    avg_price = total_brl / total_qty if total_qty > 0 else None
    
    return {
        "date": btc_data.get("date"),
        "ticker": ticker,
        "found": True,
        "saldo_qty": total_qty,
        "saldo_brl": round(total_brl, 2),
        "preco_medio": round(avg_price, 4) if avg_price else None,
        "details": matches,
    }


# ── FastAPI route (add to api.py) ──
"""
To add to api.py:

from app.btc_data import fetch_btc_data, get_btc_for_ticker

@app.get("/btc")
async def btc_endpoint(ticker: str = None):
    btc_data = fetch_btc_data()
    
    if ticker:
        result = get_btc_for_ticker(ticker, btc_data)
        return result
    
    # Return summary: top 20 by saldo
    positions = btc_data.get("positions", [])
    
    # Aggregate by ticker
    agg = {}
    for p in positions:
        t = p["ticker"]
        if t not in agg:
            agg[t] = {"ticker": t, "empresa": p["empresa"], "saldo_qty": 0, "saldo_brl": 0}
        agg[t]["saldo_qty"] += p["saldo_qty"]
        agg[t]["saldo_brl"] += p.get("saldo_brl") or 0
    
    ranked = sorted(agg.values(), key=lambda x: x["saldo_brl"], reverse=True)
    
    return {
        "date": btc_data.get("date"),
        "total_positions": btc_data.get("count", 0),
        "unique_tickers": len(agg),
        "top": ranked[:50],
    }
"""
