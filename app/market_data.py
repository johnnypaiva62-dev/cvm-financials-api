"""
Módulo para dados de mercado — BRAPI (primário) + Yahoo Finance (fallback).

BRAPI: API brasileira com dados diretos da B3 (mais precisos).
Yahoo Finance: Fallback caso BRAPI falhe ou token não configurado.

Token BRAPI: variável de ambiente BRAPI_TOKEN.
"""

import logging
import math
import os
from datetime import datetime

logger = logging.getLogger(__name__)

BRAPI_TOKEN = os.environ.get("BRAPI_TOKEN", "")
BRAPI_BASE = "https://brapi.dev/api"


def _safe(val):
    """Retorna None se valor inválido."""
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _safe_round(val, n=2):
    v = _safe(val)
    if v is None:
        return None
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


# ============================================================
# BRAPI — Fonte primária
# ============================================================

def _fetch_brapi(ticker: str) -> dict | None:
    """
    Busca dados via BRAPI API.
    Retorna dict com dados ou None se falhar.
    """
    import requests

    if not BRAPI_TOKEN:
        logger.info("BRAPI_TOKEN não configurado, pulando BRAPI")
        return None

    try:
        url = f"{BRAPI_BASE}/quote/{ticker}"
        params = {
            "token": BRAPI_TOKEN,
            "range": "5y",
            "interval": "1wk",
            "fundamental": "true",
            "dividends": "true",
            "modules": ",".join([
                "summaryProfile",
                "defaultKeyStatistics",
                "financialData",
            ]),
        }

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        if not results:
            logger.warning(f"BRAPI: sem resultados para {ticker}")
            return None

        return results[0]

    except Exception as e:
        logger.warning(f"BRAPI falhou para {ticker}: {e}")
        return None


def _parse_brapi(raw: dict, ticker: str) -> dict:
    """Converte resposta BRAPI para nosso formato padronizado."""

    result = {
        "source": "brapi",
        "ticker": ticker.upper(),
    }

    # ── Profile ──
    sp = raw.get("summaryProfile", {}) or {}
    result["profile"] = {
        "nome": _safe(raw.get("longName") or raw.get("shortName")),
        "setor": _safe(sp.get("sectorDisp") or sp.get("sector") or raw.get("sector")),
        "industria": _safe(sp.get("industryDisp") or sp.get("industry")),
        "website": _safe(sp.get("website")),
        "employees": _safe(sp.get("fullTimeEmployees")),
        "descricao": _safe(sp.get("longBusinessSummary")),
        "moeda": _safe(raw.get("currency", "BRL")),
    }

    # ── Price ──
    result["price"] = {
        "atual": _safe_round(raw.get("regularMarketPrice")),
        "anterior": _safe_round(raw.get("regularMarketPreviousClose")),
        "variacao": _safe_round(raw.get("regularMarketChange")),
        "variacao_pct": _safe_round(raw.get("regularMarketChangePercent")),
        "high_52w": _safe_round(raw.get("fiftyTwoWeekHigh")),
        "low_52w": _safe_round(raw.get("fiftyTwoWeekLow")),
        "beta": _safe_round(raw.get("beta")),
        "media_50d": _safe_round(raw.get("fiftyDayAverage")),
        "media_200d": _safe_round(raw.get("twoHundredDayAverage")),
    }

    # ── Key Statistics ──
    ks = raw.get("defaultKeyStatistics", {}) or {}
    # BRAPI pode retornar lista ou dict
    ks_entries = {}
    if isinstance(ks, list):
        for entry in ks:
            if entry.get("type") == "ttm":
                ks_entries = entry
                break
        if not ks_entries and ks:
            ks_entries = ks[0]
    elif isinstance(ks, dict):
        ks_entries = ks

    # ── Financial Data ──
    fd = raw.get("financialData", {}) or {}

    # ── Valuation (TTM) ──
    market_cap = _safe(raw.get("marketCap"))
    ev = _safe(ks_entries.get("enterpriseValue"))
    shares = _safe(ks_entries.get("sharesOutstanding") or raw.get("sharesOutstanding"))

    result["valuation"] = {
        "market_cap": market_cap,
        "enterprise_value": ev,
        "shares_outstanding": shares,
        "float_shares": _safe(ks_entries.get("floatShares")),
        "pe_ttm": _safe_round(ks_entries.get("trailingPE") or raw.get("trailingPE")),
        "pb": _safe_round(ks_entries.get("priceToBook") or raw.get("priceToBook")),
        "ev_ebitda": _safe_round(ks_entries.get("enterpriseToEbitda")),
        "ev_revenue": _safe_round(ks_entries.get("enterpriseToRevenue")),
        "price_to_sales": _safe_round(raw.get("priceToSalesTrailing12Months")),
        "peg": _safe_round(ks_entries.get("pegRatio")),
    }

    # ── Valuation Forward ──
    result["valuation_forward"] = {
        "pe_forward": _safe_round(ks_entries.get("forwardPE") or raw.get("forwardPE")),
        "eps_forward": _safe_round(ks_entries.get("forwardEps")),
        "price_target_mean": _safe_round(fd.get("targetMeanPrice") or raw.get("targetMeanPrice")),
        "price_target_high": _safe_round(fd.get("targetHighPrice") or raw.get("targetHighPrice")),
        "price_target_low": _safe_round(fd.get("targetLowPrice") or raw.get("targetLowPrice")),
        "recommendation": _safe(fd.get("recommendationKey") or raw.get("recommendationKey")),
        "num_analysts": _safe(fd.get("numberOfAnalystOpinions") or raw.get("numberOfAnalystOpinions")),
    }

    # ── Dividends ──
    div_yield = _safe(raw.get("dividendYield"))
    div_rate = _safe(raw.get("dividendRate"))
    payout = _safe(ks_entries.get("payoutRatio"))

    # Normaliza: se > 1 já é %, se < 1 é decimal
    def _to_pct(v):
        if v is None:
            return None
        return v if v > 1 else v * 100

    result["dividends"] = {
        "yield": _safe_round(_to_pct(div_yield)),
        "rate": _safe_round(div_rate),
        "payout": _safe_round(_to_pct(payout)),
        "ex_date": None,
    }

    # Ex-date do último dividendo
    divs_data = raw.get("dividendsData", {})
    if isinstance(divs_data, dict):
        cash_divs = divs_data.get("cashDividends", [])
        if cash_divs and isinstance(cash_divs, list):
            result["dividends"]["ex_date"] = _safe(cash_divs[0].get("exDate"))

    # ── Price History ──
    hist = raw.get("historicalDataPrice", [])
    records = []
    if hist and isinstance(hist, list):
        for h in hist:
            dt_val = h.get("date")
            if isinstance(dt_val, (int, float)):
                dt_str = datetime.fromtimestamp(dt_val).strftime("%Y-%m-%d")
            elif isinstance(dt_val, str):
                dt_str = dt_val[:10]
            else:
                continue
            records.append({
                "dt": dt_str,
                "open": _safe_round(h.get("open")),
                "high": _safe_round(h.get("high")),
                "low": _safe_round(h.get("low")),
                "close": _safe_round(h.get("close")),
                "volume": int(h.get("volume", 0) or 0),
            })
        records.sort(key=lambda x: x["dt"])

    result["price_history"] = records

    return result


# ============================================================
# YAHOO FINANCE — Fallback
# ============================================================

def _fetch_yahoo(ticker: str) -> dict:
    """Busca dados via yfinance (fallback)."""
    import yfinance as yf

    yf_ticker = ticker.upper()
    if not yf_ticker.endswith(".SA"):
        yf_ticker += ".SA"

    result = {
        "source": "yahoo",
        "ticker": ticker.upper(),
        "profile": {},
        "price": {},
        "valuation": {},
        "valuation_forward": {},
        "dividends": {},
        "price_history": [],
    }

    try:
        t = yf.Ticker(yf_ticker)
        info = t.info or {}

        result["profile"] = {
            "nome": _safe(info.get("longName") or info.get("shortName")),
            "setor": _safe(info.get("sector")),
            "industria": _safe(info.get("industry")),
            "website": _safe(info.get("website")),
            "employees": _safe(info.get("fullTimeEmployees")),
            "descricao": _safe(info.get("longBusinessSummary")),
            "moeda": _safe(info.get("currency", "BRL")),
        }

        price = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        prev_close = _safe(info.get("previousClose") or info.get("regularMarketPreviousClose"))
        result["price"] = {
            "atual": _safe_round(price),
            "anterior": _safe_round(prev_close),
            "variacao": _safe_round(price - prev_close) if price and prev_close else None,
            "variacao_pct": _safe_round(((price - prev_close) / prev_close) * 100) if price and prev_close and prev_close != 0 else None,
            "high_52w": _safe_round(info.get("fiftyTwoWeekHigh")),
            "low_52w": _safe_round(info.get("fiftyTwoWeekLow")),
            "beta": _safe_round(info.get("beta")),
            "media_50d": _safe_round(info.get("fiftyDayAverage")),
            "media_200d": _safe_round(info.get("twoHundredDayAverage")),
        }

        market_cap = _safe(info.get("marketCap"))
        ev = _safe(info.get("enterpriseValue"))
        result["valuation"] = {
            "market_cap": market_cap,
            "enterprise_value": ev,
            "shares_outstanding": _safe(info.get("sharesOutstanding")),
            "float_shares": _safe(info.get("floatShares")),
            "pe_ttm": _safe_round(info.get("trailingPE")),
            "pb": _safe_round(info.get("priceToBook")),
            "ev_ebitda": _safe_round(info.get("enterpriseToEbitda")),
            "ev_revenue": _safe_round(info.get("enterpriseToRevenue")),
            "price_to_sales": _safe_round(info.get("priceToSalesTrailing12Months")),
            "peg": _safe_round(info.get("pegRatio")),
        }

        result["valuation_forward"] = {
            "pe_forward": _safe_round(info.get("forwardPE")),
            "eps_forward": _safe_round(info.get("forwardEps")),
            "price_target_mean": _safe_round(info.get("targetMeanPrice")),
            "price_target_high": _safe_round(info.get("targetHighPrice")),
            "price_target_low": _safe_round(info.get("targetLowPrice")),
            "recommendation": _safe(info.get("recommendationKey")),
            "num_analysts": _safe(info.get("numberOfAnalystOpinions")),
        }

        div_yield = _safe(info.get("dividendYield"))
        payout = _safe(info.get("payoutRatio"))
        result["dividends"] = {
            "yield": _safe_round(div_yield * 100 if div_yield and div_yield < 1 else div_yield),
            "rate": _safe_round(info.get("dividendRate")),
            "payout": _safe_round(payout * 100 if payout and payout < 1 else payout),
            "ex_date": str(info.get("exDividendDate", "")) if info.get("exDividendDate") else None,
        }

        try:
            hist = t.history(period="5y", interval="1wk")
            if hist is not None and not hist.empty:
                records = []
                for date, row in hist.iterrows():
                    records.append({
                        "dt": date.strftime("%Y-%m-%d"),
                        "open": _safe_round(row.get("Open")),
                        "high": _safe_round(row.get("High")),
                        "low": _safe_round(row.get("Low")),
                        "close": _safe_round(row.get("Close")),
                        "volume": int(row.get("Volume", 0)),
                    })
                result["price_history"] = records
        except Exception as e:
            logger.warning(f"Yahoo: erro no histórico: {e}")

    except Exception as e:
        logger.error(f"Yahoo falhou para {ticker}: {e}")
        result["error"] = str(e)

    return result


# ============================================================
# INTERFACE PÚBLICA — BRAPI com fallback Yahoo
# ============================================================

def get_market_data(ticker: str) -> dict:
    """
    Busca dados de mercado: BRAPI (primário) + Yahoo (complementar para NTM).
    Combina o melhor de cada fonte.

    Args:
        ticker: Ticker B3 (ex: "VALE3")

    Returns:
        Dict padronizado com dados de mercado + campo "source"
    """
    brapi_data = None
    yahoo_data = None

    # Tenta BRAPI primeiro
    brapi_raw = _fetch_brapi(ticker)
    if brapi_raw:
        logger.info(f"✓ Dados BRAPI obtidos para {ticker}")
        brapi_data = _parse_brapi(brapi_raw, ticker)

    # Busca Yahoo para NTM / complemento
    try:
        yahoo_data = _fetch_yahoo(ticker)
        logger.info(f"✓ Dados Yahoo obtidos para {ticker}")
    except Exception as e:
        logger.warning(f"Yahoo indisponível para {ticker}: {e}")

    # Se só tem Yahoo, retorna Yahoo
    if not brapi_data:
        return yahoo_data or {"source": "none", "ticker": ticker}

    # Se só tem BRAPI, retorna BRAPI
    if not yahoo_data:
        return brapi_data

    # ── Combina: BRAPI base + Yahoo NTM ──
    combined = {**brapi_data}
    combined["source"] = "brapi+yahoo"

    # BRAPI = preços nominais, Yahoo = preços ajustados por proventos
    combined["price_history"] = brapi_data.get("price_history", [])  # nominal
    combined["price_history_adjusted"] = yahoo_data.get("price_history", [])  # ajustado

    # Forward/NTM: pega do Yahoo se BRAPI não tem
    brapi_fwd = brapi_data.get("valuation_forward", {})
    yahoo_fwd = yahoo_data.get("valuation_forward", {})
    merged_fwd = {}
    for key in set(list(brapi_fwd.keys()) + list(yahoo_fwd.keys())):
        merged_fwd[key] = brapi_fwd.get(key) or yahoo_fwd.get(key)
    combined["valuation_forward"] = merged_fwd

    # Complementa valuation com dados Yahoo se BRAPI não tem
    brapi_val = brapi_data.get("valuation", {})
    yahoo_val = yahoo_data.get("valuation", {})
    for key in yahoo_val:
        if not brapi_val.get(key) and yahoo_val.get(key):
            brapi_val[key] = yahoo_val[key]
    combined["valuation"] = brapi_val

    return combined


def get_combined_overview(cvm_overview: dict, market_data: dict) -> dict:
    """
    Combina dados CVM (fundamentals) com dados de mercado (BRAPI/Yahoo).

    Args:
        cvm_overview: Dict retornado por compute_overview()
        market_data: Dict retornado por get_market_data()

    Returns:
        Dict combinado
    """
    combined = {**cvm_overview}

    combined["source_market"] = market_data.get("source", "unknown")
    combined["profile"] = {
        **cvm_overview.get("company", {}),
        **{k: v for k, v in market_data.get("profile", {}).items() if v is not None},
    }
    combined["price"] = market_data.get("price", {})
    combined["valuation"] = market_data.get("valuation", {})
    combined["valuation_forward"] = market_data.get("valuation_forward", {})
    combined["dividends_market"] = market_data.get("dividends", {})
    combined["price_history"] = market_data.get("price_history", [])
    combined["price_history_adjusted"] = market_data.get("price_history_adjusted", [])

    # Múltiplos recalculados com dados CVM
    val = market_data.get("valuation", {})
    margins = cvm_overview.get("margins", {})
    cf = cvm_overview.get("cash_flow", {})
    fh = cvm_overview.get("financial_health", {})

    market_cap = val.get("market_cap")
    ev = val.get("enterprise_value")
    receita = margins.get("receita")

    # ── Recalcula EV com dados CVM (mais confiável) ──
    # EV = Market Cap + Dívida Líquida
    divida_liquida = fh.get("divida_liquida")
    if market_cap and divida_liquida is not None:
        ev_cvm = market_cap + divida_liquida
        combined["valuation"]["enterprise_value_cvm"] = ev_cvm
        # Usa EV calculado pela CVM como principal
        ev = ev_cvm
        combined["valuation"]["enterprise_value"] = ev_cvm
        logger.info(f"EV recalculado via CVM: {ev_cvm:,.0f} (Market Cap {market_cap:,.0f} + Dív. Líq. {divida_liquida:,.0f})")

    calc = {}
    if market_cap and receita and receita != 0:
        calc["price_to_sales"] = _safe_round(market_cap / receita)
    if ev and receita and receita != 0:
        calc["ev_sales"] = _safe_round(ev / receita)
        # Recalcula EV/EBITDA com EBIT como proxy (se disponível)
        ebit = margins.get("ebit_value")
        if ebit and ebit != 0:
            calc["ev_ebit"] = _safe_round(ev / ebit)

    fcf = cf.get("fcf")
    if market_cap and fcf and fcf != 0:
        calc["price_to_fcf"] = _safe_round(market_cap / fcf)

    combined["valuation_calculated"] = calc

    return combined
