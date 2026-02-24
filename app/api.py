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
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.service import data_service
from app.overview import compute_overview
from app.market_data import get_market_data, get_combined_overview

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
