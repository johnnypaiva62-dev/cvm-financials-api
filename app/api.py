"""
API REST para dados financeiros de empresas brasileiras (CVM).

Endpoints:
    GET /                    — Health check
    GET /status              — Status do serviço
    GET /search?q=PETR4      — Busca por ticker ou nome
    GET /ticker/{ticker}     — Dados completos por ticker
    GET /empresas            — Lista de empresas
    GET /empresa/{cd_cvm}    — Dados por código CVM
    GET /dre                 — DRE (filtros opcionais)
    GET /balanco/ativo       — Balanço Patrimonial Ativo
    GET /balanco/passivo     — Balanço Patrimonial Passivo
    GET /dfc                 — Demonstração do Fluxo de Caixa
    GET /contas/{statement}  — Contas disponíveis
    POST /reload             — Recarrega dados da CVM
"""

import logging
import threading
import traceback
import math
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.service import data_service
from app.overview import compute_overview, compute_indicadores
from app.market_data import get_market_data, get_combined_overview, fetch_long_term_prices, fetch_batch_market_data
from app.ticker_mapper import ticker_mapper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================
# API KEY AUTH
# ============================================================
API_KEY = os.getenv("API_KEY")
_PUBLIC_PATHS = {"/", "/status", "/docs", "/openapi.json", "/redoc"}


class ApiKeyMiddleware:
    """Pure ASGI middleware — compatible with CORSMiddleware."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            req = Request(scope)
            if API_KEY and req.method != "OPTIONS" and req.url.path not in _PUBLIC_PATHS:
                key = req.headers.get("x-api-key")
                if key != API_KEY:
                    resp = JSONResponse(status_code=401, content={"detail": "API key inválida ou ausente"})
                    await resp(scope, receive, send)
                    return
        await self.app(scope, receive, send)


# ============================================================
# CARREGAMENTO EM BACKGROUND
# ============================================================
# O Railway mata o processo se ele não responder rápido.
# Por isso carregamos os dados em uma thread separada.

_loading = False
_load_error: str | None = None


def _background_load():
    """Carrega dados da CVM em background."""
    global _loading, _load_error
    _loading = True
    _load_error = None
    try:
        data_service.load(use_cache=True)
        logger.info("✓ Dados carregados com sucesso!")
    except Exception as e:
        _load_error = str(e)
        logger.error(f"✗ Erro ao carregar dados: {e}")
    finally:
        _loading = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Inicia carregamento em background no startup."""
    logger.info("Iniciando carga de dados em background...")
    thread = threading.Thread(target=_background_load, daemon=True)
    thread.start()
    yield
    logger.info("Encerrando API...")


# ============================================================
# APP
# ============================================================

app = FastAPI(
    title="CVM Financials API",
    description=(
        "API para consulta de demonstrativos financeiros "
        "(DRE, Balanço Patrimonial, Fluxo de Caixa) "
        "de empresas listadas na B3, com dados da CVM."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# Middleware order: last added = outermost = first to execute
# Request → CORSMiddleware → ApiKeyMiddleware → Routes
app.add_middleware(ApiKeyMiddleware)  # Inner
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)  # Outer — wraps everything, including 401 responses


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Captura erros não tratados e retorna detalhes úteis."""
    tb = traceback.format_exc()
    logger.error(f"Erro em {request.url}: {exc}\n{tb}")
    return JSONResponse(
        status_code=500,
        content={
            "detail": f"Erro interno: {str(exc)}",
            "path": str(request.url),
            "type": type(exc).__name__,
        },
    )


# ============================================================
# HELPERS
# ============================================================

def _clean_records(records: list[dict]) -> list[dict]:
    """Remove NaN/inf de records — JSON não aceita esses valores."""
    for record in records:
        for key, value in record.items():
            if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                record[key] = None
    return records


def df_to_response(df, limit: int = 1000, offset: int = 0) -> dict:
    """Converte DataFrame para resposta JSON com paginação."""
    if df is None or df.empty:
        return {"data": [], "total": 0, "limit": limit, "offset": offset}

    total = len(df)
    page = df.iloc[offset : offset + limit].copy()

    for col in page.columns:
        if hasattr(page[col], "dt"):
            try:
                page[col] = page[col].dt.strftime("%Y-%m-%d")
            except Exception:
                page[col] = page[col].astype(str)

    records = _clean_records(page.to_dict(orient="records"))

    return {
        "data": records,
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def check_loaded():
    """Verifica se os dados estão carregados."""
    if _loading:
        raise HTTPException(
            status_code=503,
            detail="Dados ainda carregando. Aguarde 2-3 minutos e tente novamente.",
        )
    if not data_service.loaded:
        detail = "Dados não carregados."
        if _load_error:
            detail += f" Erro: {_load_error}"
        raise HTTPException(status_code=503, detail=detail)


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    """Health check — sempre responde, mesmo durante carregamento."""
    if _loading:
        status = "loading"
    elif data_service.loaded:
        status = "ready"
    else:
        status = "error"

    return {
        "service": "CVM Financials API",
        "status": status,
        "docs": "/docs",
        "error": _load_error,
    }


@app.get("/status")
async def status():
    """Status detalhado do serviço."""
    base = data_service.get_status()
    base["loading"] = _loading
    base["load_error"] = _load_error
    return base


@app.post("/reload")
async def reload(
    use_cache: bool = Query(True, description="Usar cache local"),
):
    """Recarrega dados da CVM (em background)."""
    global _loading
    if _loading:
        return {"status": "already_loading", "message": "Carga já em andamento."}

    def _reload():
        global _loading, _load_error
        _loading = True
        _load_error = None
        try:
            data_service.load(use_cache=use_cache)
        except Exception as e:
            _load_error = str(e)
        finally:
            _loading = False

    thread = threading.Thread(target=_reload, daemon=True)
    thread.start()
    return {"status": "ok", "message": "Recarga iniciada em background."}


# --- BUSCA POR TICKER ---

@app.get("/search")
async def search_ticker(
    q: str = Query(..., min_length=2, description="Ticker ou nome (ex: PETR4, Petrobras)"),
):
    """
    Busca empresas por ticker ou nome.
    Retorna ticker(s), código CVM, CNPJ e nome.
    """
    check_loaded()
    results = data_service.search_ticker(q)
    return {"results": results, "total": len(results)}


# --- EMPRESA POR TICKER ---

@app.get("/ticker/{ticker}")
async def company_by_ticker(ticker: str):
    """
    Retorna todos os demonstrativos pelo ticker B3.
    Exemplo: /ticker/PETR4
    """
    check_loaded()
    result = data_service.get_company_financials(ticker=ticker)

    has_data = any(len(v) > 0 for v in result.values())
    if not has_data:
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker.upper()}' não encontrado. Use /search?q={ticker} para buscar.",
        )
    return result


# --- OVERVIEW ---

@app.get("/overview")
async def get_overview(
    ticker: str | None = Query(None, description="Ticker B3 (ex: PETR4)"),
    cnpj: str | None = Query(None, description="CNPJ"),
    cd_cvm: str | None = Query(None, description="Código CVM"),
    include_market: bool = Query(True, description="Incluir dados de mercado (Yahoo Finance)"),
):
    """
    Overview financeiro completo de uma empresa.
    Combina dados CVM (fundamentals) com Yahoo Finance (mercado).
    Exemplo: /overview?ticker=VALE3
    """
    check_loaded()
    if not ticker and not cnpj and not cd_cvm:
        raise HTTPException(status_code=400, detail="Informe ticker, cnpj ou cd_cvm.")

    # Resolve ticker se necessário (para buscar dados de mercado)
    resolved_ticker = ticker
    if not resolved_ticker and (cnpj or cd_cvm):
        # Tenta encontrar o ticker pelo ticker_mapper
        try:
            from app.ticker_mapper import ticker_mapper
            if cd_cvm:
                # Busca reversa: cd_cvm → ticker
                for tk, info in ticker_mapper._ticker_to_cvm.items():
                    if str(info.get("cd_cvm", "")).lstrip("0") == str(cd_cvm).lstrip("0"):
                        resolved_ticker = tk
                        break
            if not resolved_ticker:
                results = data_service.search_ticker(cnpj or cd_cvm or "")
                if results:
                    resolved_ticker = results[0].get("ticker")
        except Exception as e:
            logger.warning(f"Não conseguiu resolver ticker: {e}")

    # Busca demonstrativos CVM
    dre_annual = data_service.get_statement("DRE", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm, periodo="anual")
    dre_tri = data_service.get_statement("DRE", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm, periodo="trimestral")
    bpa = data_service.get_statement("BPA", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm)
    bpp = data_service.get_statement("BPP", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm)
    dfc_annual = data_service.get_statement("DFC", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm, periodo="anual")

    overview = compute_overview(
        dre_df=dre_tri,
        bpa_df=bpa,
        bpp_df=bpp,
        dfc_df=dfc_annual,
        dre_annual_df=dre_annual,
    )

    # Combina com Yahoo Finance se ticker disponível
    if include_market and resolved_ticker:
        try:
            market = get_market_data(resolved_ticker)
            overview = get_combined_overview(overview, market)
        except Exception as e:
            logger.warning(f"Erro ao buscar dados de mercado: {e}")
            overview["market_error"] = str(e)

    # Limpa NaN
    def clean(obj):
        if isinstance(obj, dict):
            return {k: clean(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [clean(i) for i in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    return clean(overview)


@app.get("/indicadores")
async def get_indicadores(
    ticker: str | None = Query(None),
    cnpj: str | None = Query(None),
    cd_cvm: str | None = Query(None),
    periodo: str = Query("anual", description="anual ou trimestral"),
    include_market: bool = Query(True, description="Incluir valuation (requer ticker)"),
):
    """
    Indicadores financeiros computados por período.
    Combina DRE + BPA + BPP + DFC para calcular margens, retornos, alavancagem, valuation.
    Exemplo: /indicadores?ticker=VALE3&periodo=anual
    """
    check_loaded()
    if not ticker and not cnpj and not cd_cvm:
        raise HTTPException(status_code=400, detail="Informe ticker, cnpj ou cd_cvm.")

    p = periodo.lower().strip()
    dre = data_service.get_statement("DRE", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm, periodo=p)
    bpa = data_service.get_statement("BPA", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm)
    bpp = data_service.get_statement("BPP", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm)
    dfc = data_service.get_statement("DFC", ticker=ticker, cnpj=cnpj, cd_cvm=cd_cvm, periodo=p)

    # Fetch long-term monthly prices for valuation multiples
    price_history = None
    shares = None
    resolved_ticker = ticker
    if include_market and resolved_ticker:
        try:
            price_history, shares = fetch_long_term_prices(resolved_ticker)
            logger.info(f"Indicadores: {len(price_history or [])} preços mensais, shares={shares}")
        except Exception as e:
            logger.warning(f"Erro market data para indicadores: {e}")

    indicadores = compute_indicadores(dre, bpa, bpp, dfc, price_history=price_history, shares_outstanding=shares)

    # Limpa NaN
    def clean_val(v):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v

    cleaned = [{k: clean_val(v) for k, v in d.items()} for d in indicadores]
    return {"data": cleaned, "total": len(cleaned), "periodo": p}


# --- EMPRESAS ---

@app.get("/empresas")
async def list_companies(
    search: str | None = Query(None, description="Busca por nome"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Lista empresas disponíveis."""
    check_loaded()
    df = data_service.get_companies(search=search)
    return df_to_response(df, limit, offset)


@app.get("/empresa/{cd_cvm}")
async def company_financials(cd_cvm: str):
    """Retorna todos os demonstrativos por código CVM."""
    check_loaded()
    result = data_service.get_company_financials(cd_cvm=cd_cvm)

    has_data = any(len(v) > 0 for v in result.values())
    if not has_data:
        raise HTTPException(
            status_code=404,
            detail=f"Empresa com CD_CVM={cd_cvm} não encontrada.",
        )
    return result


# --- DRE ---

@app.get("/dre")
async def get_dre(
    ticker: str | None = Query(None, description="Ticker B3 (ex: PETR4)"),
    cnpj: str | None = Query(None, description="CNPJ"),
    cd_cvm: str | None = Query(None, description="Código CVM"),
    dt_refer: str | None = Query(None, description="Data referência (YYYY-MM-DD)"),
    periodo: str | None = Query(None, description="trimestral ou anual"),
    raw: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """DRE. Exemplo: /dre?ticker=PETR4&periodo=anual"""
    check_loaded()
    df = data_service.get_statement(
        "DRE", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, periodo=periodo, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


# --- BALANÇO ---

@app.get("/balanco/ativo")
async def get_bpa(
    ticker: str | None = Query(None, description="Ticker B3"),
    cnpj: str | None = Query(None),
    cd_cvm: str | None = Query(None),
    dt_refer: str | None = Query(None),
    periodo: str | None = Query(None, description="trimestral ou anual"),
    raw: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Balanço Patrimonial — Ativo. Exemplo: /balanco/ativo?ticker=VALE3"""
    check_loaded()
    df = data_service.get_statement(
        "BPA", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, periodo=periodo, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


@app.get("/balanco/passivo")
async def get_bpp(
    ticker: str | None = Query(None, description="Ticker B3"),
    cnpj: str | None = Query(None),
    cd_cvm: str | None = Query(None),
    dt_refer: str | None = Query(None),
    periodo: str | None = Query(None, description="trimestral ou anual"),
    raw: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Balanço — Passivo e PL. Exemplo: /balanco/passivo?ticker=ITUB4"""
    check_loaded()
    df = data_service.get_statement(
        "BPP", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, periodo=periodo, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


# --- DFC ---

@app.get("/dfc")
async def get_dfc(
    ticker: str | None = Query(None, description="Ticker B3"),
    cnpj: str | None = Query(None),
    cd_cvm: str | None = Query(None),
    dt_refer: str | None = Query(None),
    periodo: str | None = Query(None, description="trimestral ou anual"),
    raw: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Fluxo de Caixa. Exemplo: /dfc?ticker=BBAS3"""
    check_loaded()
    df = data_service.get_statement(
        "DFC", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, periodo=periodo, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


# --- CONTAS ---

@app.get("/contas/{statement}")
async def available_accounts(statement: str):
    """Lista contas rastreadas. statement = DRE | BPA | BPP | DFC_MI | DFC_MD"""
    valid = ["DRE", "BPA", "BPP", "DFC_MI", "DFC_MD"]
    if statement.upper() not in valid:
        raise HTTPException(status_code=400, detail=f"Use: {valid}")
    return data_service.get_available_accounts(statement.upper())


# --- SCREENER ---

_screener_cache: dict = {"data": None, "ts": None}


@app.get("/screener")
async def get_screener(
    force: bool = Query(False, description="Força recálculo do cache"),
):
    """
    Screener de ações: retorna indicadores mais recentes (anuais)
    para todas as empresas mapeadas com ticker.
    
    Computa DRE + BPA + BPP + DFC → indicadores para cada empresa.
    Resultado é cacheado em memória.
    """
    check_loaded()

    # Usa cache se disponível (menos de 24h)
    import time
    if not force and _screener_cache["data"] and _screener_cache["ts"]:
        age = time.time() - _screener_cache["ts"]
        if age < 86400:  # 24h
            return _screener_cache["data"]

    companies = ticker_mapper.get_all_mapped_companies()
    logger.info(f"Screener: computando indicadores para {len(companies)} empresas")

    results = []
    errors = []

    for comp in companies:
        try:
            cd_cvm = comp["cd_cvm"]
            cnpj = comp["cnpj"]

            # Get latest annual data
            dre = data_service.get_statement("DRE", cd_cvm=cd_cvm, periodo="anual")
            bpa = data_service.get_statement("BPA", cd_cvm=cd_cvm)
            bpp = data_service.get_statement("BPP", cd_cvm=cd_cvm)
            dfc = data_service.get_statement("DFC", cd_cvm=cd_cvm, periodo="anual")

            indicadores = compute_indicadores(dre, bpa, bpp, dfc)
            if not indicadores:
                continue

            # Get latest period
            latest = indicadores[-1]  # Already sorted by date

            # Clean NaN/inf
            cleaned = {}
            for k, v in latest.items():
                if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                    cleaned[k] = None
                else:
                    cleaned[k] = v

            # Add company metadata
            row = {
                "ticker": comp["ticker"],
                "tickers": comp["tickers"],
                "nome": comp["nome"],
                "setor": comp["setor"],
                "cd_cvm": cd_cvm,
                **cleaned,
            }
            results.append(row)

        except Exception as e:
            errors.append({"ticker": comp.get("ticker", "?"), "error": str(e)})
            logger.debug(f"Screener: erro em {comp.get('ticker')}: {e}")

    # ── Fetch batch market data for valuation ──
    tickers_with_results = [r["ticker"] for r in results]
    logger.info(f"Screener: buscando market data para {len(tickers_with_results)} tickers")
    try:
        market_batch = fetch_batch_market_data(tickers_with_results)
    except Exception as e:
        logger.warning(f"Screener: batch market data falhou: {e}")
        market_batch = {}

    # Add valuation metrics to each company
    for row in results:
        ticker = row["ticker"]
        md = market_batch.get(ticker, {})
        price = md.get("price")
        shares = md.get("shares")
        mc = md.get("market_cap")
        ev_direct = md.get("enterprise_value")

        if not mc and price and shares:
            mc = price * shares

        dl = row.get("Dívida Líquida") or 0
        ev = ev_direct if ev_direct else (mc + dl if mc else None)

        row["Preço"] = round(price, 2) if price else None
        row["Market Cap"] = round(mc, 0) if mc else None
        row["EV"] = round(ev, 0) if ev else None

        # Multiples (using latest annual fundamentals + current price)
        rec = row.get("Receita Líquida")
        luc = row.get("Lucro Líquido")
        ebitda = row.get("EBITDA")
        ebit = row.get("EBIT")
        pl = row.get("Patrimônio Líquido")
        fcf = row.get("FCF")

        row["P/E"] = round(mc / luc, 1) if mc and luc and luc > 0 else None
        row["P/B"] = round(mc / pl, 1) if mc and pl and pl > 0 else None
        row["EV/EBITDA"] = round(ev / ebitda, 1) if ev and ebitda and ebitda > 0 else None
        row["EV/EBIT"] = round(ev / ebit, 1) if ev and ebit and ebit > 0 else None
        row["EV/Sales"] = round(ev / rec, 1) if ev and rec and rec > 0 else None
        row["P/Sales"] = round(mc / rec, 1) if mc and rec and rec > 0 else None
        row["P/FCF"] = round(mc / fcf, 1) if mc and fcf and fcf > 0 else None

    # Collect all unique metric keys
    all_keys = set()
    for r in results:
        for k in r:
            if k not in ("ticker", "tickers", "nome", "setor", "cd_cvm", "DT_REFER"):
                all_keys.add(k)

    # Sort metrics by type
    metric_defs = []
    IND_ORDER = [
        "Receita Líquida", "Resultado Bruto", "EBITDA", "EBIT", "Lucro Líquido",
        "Market Cap", "EV", "Preço",
        "P/E", "P/B", "EV/EBITDA", "EV/EBIT", "EV/Sales", "P/Sales", "P/FCF",
        "Margem Bruta", "Margem EBITDA", "Margem EBIT", "Margem Líquida", "Margem FCF",
        "ROE", "ROA", "ROIC",
        "Dívida Líquida", "Dívida Bruta", "Patrimônio Líquido",
        "Dív.Líq/EBITDA", "Dív.Líq/EBIT", "Dív.Líq/PL", "Dív.Bruta/PL",
        "EBIT/Desp.Fin", "EBITDA/Desp.Fin",
        "FCO", "CAPEX", "FCF", "Dividendos Pagos",
    ]
    sorted_keys = [k for k in IND_ORDER if k in all_keys]
    # Add any remaining keys not in the order
    sorted_keys += [k for k in sorted(all_keys) if k not in sorted_keys]

    response = {
        "data": results,
        "total": len(results),
        "errors": len(errors),
        "metrics": sorted_keys,
        "setores": sorted(set(r["setor"] for r in results if r.get("setor"))),
    }

    _screener_cache["data"] = response
    _screener_cache["ts"] = time.time()

    logger.info(f"Screener: {len(results)} empresas, {len(errors)} erros")
    return response
