"""
Módulo para dados de mercado via Yahoo Finance (yfinance).

Fornece:
    - Preço atual e histórico
    - Market Cap, EV, Shares Outstanding
    - Múltiplos (P/E, P/B, EV/EBITDA, EV/Sales, P/FCF)
    - Setor, Indústria, Website, Employees
    - Dividend Yield, Beta
    - Forward estimates

Tickers brasileiros usam sufixo .SA (ex: VALE3.SA, PETR4.SA)
"""

import logging
import math
from functools import lru_cache
from datetime import datetime, timedelta

import yfinance as yf

logger = logging.getLogger(__name__)


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
    return round(v, n)


def get_market_data(ticker: str) -> dict:
    """
    Busca dados de mercado do Yahoo Finance para um ticker brasileiro.

    Args:
        ticker: Ticker B3 (ex: "VALE3"). O sufixo .SA é adicionado automaticamente.

    Returns:
        Dict com dados de mercado
    """
    # Garante sufixo .SA
    yf_ticker = ticker.upper()
    if not yf_ticker.endswith(".SA"):
        yf_ticker += ".SA"

    result = {
        "ticker": ticker.upper(),
        "yf_ticker": yf_ticker,
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

        # ── Profile ──
        result["profile"] = {
            "nome": _safe(info.get("longName") or info.get("shortName")),
            "setor": _safe(info.get("sector")),
            "industria": _safe(info.get("industry")),
            "website": _safe(info.get("website")),
            "employees": _safe(info.get("fullTimeEmployees")),
            "descricao": _safe(info.get("longBusinessSummary")),
            "moeda": _safe(info.get("currency", "BRL")),
        }

        # ── Price ──
        price = _safe(info.get("currentPrice") or info.get("regularMarketPrice"))
        prev_close = _safe(info.get("previousClose") or info.get("regularMarketPreviousClose"))
        high_52 = _safe(info.get("fiftyTwoWeekHigh"))
        low_52 = _safe(info.get("fiftyTwoWeekLow"))
        beta = _safe(info.get("beta"))

        result["price"] = {
            "atual": _safe_round(price),
            "anterior": _safe_round(prev_close),
            "variacao": _safe_round(price - prev_close) if price and prev_close else None,
            "variacao_pct": _safe_round(((price - prev_close) / prev_close) * 100) if price and prev_close and prev_close != 0 else None,
            "high_52w": _safe_round(high_52),
            "low_52w": _safe_round(low_52),
            "beta": _safe_round(beta),
            "media_50d": _safe_round(info.get("fiftyDayAverage")),
            "media_200d": _safe_round(info.get("twoHundredDayAverage")),
        }

        # ── Valuation (TTM) ──
        market_cap = _safe(info.get("marketCap"))
        ev = _safe(info.get("enterpriseValue"))
        shares = _safe(info.get("sharesOutstanding"))

        result["valuation"] = {
            "market_cap": market_cap,
            "enterprise_value": ev,
            "shares_outstanding": shares,
            "pe_ttm": _safe_round(info.get("trailingPE")),
            "pb": _safe_round(info.get("priceToBook")),
            "ev_ebitda": _safe_round(info.get("enterpriseToEbitda")),
            "ev_revenue": _safe_round(info.get("enterpriseToRevenue")),
            "price_to_sales": _safe_round(info.get("priceToSalesTrailing12Months")),
            "peg": _safe_round(info.get("pegRatio")),
        }

        # ── Valuation Forward (NTM) ──
        result["valuation_forward"] = {
            "pe_forward": _safe_round(info.get("forwardPE")),
            "eps_forward": _safe_round(info.get("forwardEps")),
            "price_target_mean": _safe_round(info.get("targetMeanPrice")),
            "price_target_high": _safe_round(info.get("targetHighPrice")),
            "price_target_low": _safe_round(info.get("targetLowPrice")),
            "recommendation": _safe(info.get("recommendationKey")),
            "num_analysts": _safe(info.get("numberOfAnalystOpinions")),
        }

        # ── Dividends ──
        div_yield = _safe(info.get("dividendYield"))
        div_rate = _safe(info.get("dividendRate"))
        payout = _safe(info.get("payoutRatio"))

        result["dividends"] = {
            "yield": _safe_round(div_yield * 100 if div_yield and div_yield < 1 else div_yield),
            "rate": _safe_round(div_rate),
            "payout": _safe_round(payout * 100 if payout and payout < 1 else payout),
            "ex_date": str(info.get("exDividendDate", "")) if info.get("exDividendDate") else None,
        }

        # ── Price History (5 years daily) ──
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
            logger.warning(f"Erro ao buscar histórico de preços: {e}")

    except Exception as e:
        logger.error(f"Erro ao buscar dados do Yahoo Finance para {yf_ticker}: {e}")
        result["error"] = str(e)

    return result


def get_combined_overview(cvm_overview: dict, market_data: dict) -> dict:
    """
    Combina dados CVM (fundamentals) com Yahoo Finance (mercado).

    Adiciona:
        - profile (setor, website, etc.)
        - price (cotação, 52w, beta)
        - valuation (P/E, P/B, EV/EBITDA — do Yahoo)
        - valuation_calculated (múltiplos recalculados com dados CVM)
        - dividends
        - price_history

    Args:
        cvm_overview: Dict retornado por compute_overview()
        market_data: Dict retornado por get_market_data()

    Returns:
        Dict combinado
    """
    combined = {**cvm_overview}

    # Adiciona dados de mercado
    combined["profile"] = {
        **cvm_overview.get("company", {}),
        **market_data.get("profile", {}),
    }
    combined["price"] = market_data.get("price", {})
    combined["valuation"] = market_data.get("valuation", {})
    combined["valuation_forward"] = market_data.get("valuation_forward", {})
    combined["dividends_market"] = market_data.get("dividends", {})
    combined["price_history"] = market_data.get("price_history", [])

    # ── Múltiplos recalculados com dados CVM ──
    # (mais confiáveis porque usam demonstrativos oficiais)
    val = market_data.get("valuation", {})
    margins = cvm_overview.get("margins", {})
    fh = cvm_overview.get("financial_health", {})
    cf = cvm_overview.get("cash_flow", {})

    market_cap = val.get("market_cap")
    ev = val.get("enterprise_value")
    receita = margins.get("receita")

    calc = {}
    if market_cap and receita and receita != 0:
        calc["price_to_sales"] = _safe_round(market_cap / receita)
    if ev and receita and receita != 0:
        calc["ev_sales"] = _safe_round(ev / receita)

    fcf = cf.get("fcf")
    if market_cap and fcf and fcf != 0:
        calc["price_to_fcf"] = _safe_round(market_cap / fcf)

    combined["valuation_calculated"] = calc

    return combined
