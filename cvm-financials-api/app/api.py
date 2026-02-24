"""
API REST para dados financeiros de empresas brasileiras (CVM).

Endpoints:
    GET /status              — Status do serviço
    GET /empresas            — Lista de empresas
    GET /empresa/{cd_cvm}    — Dados financeiros completos de uma empresa
    GET /dre                 — DRE (filtros opcionais)
    GET /balanco/ativo       — Balanço Patrimonial Ativo
    GET /balanco/passivo     — Balanço Patrimonial Passivo
    GET /dfc                 — Demonstração do Fluxo de Caixa
    GET /contas/{statement}  — Contas disponíveis por demonstrativo
    POST /reload             — Recarrega dados da CVM
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.service import data_service

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ============================================================
# LIFESPAN: carrega dados no startup
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Carrega dados da CVM no startup da API."""
    logger.info("Iniciando carga de dados da CVM...")
    try:
        data_service.load(use_cache=True)
        logger.info("✓ Dados carregados com sucesso!")
    except Exception as e:
        logger.error(f"✗ Erro ao carregar dados: {e}")
        logger.info("API iniciada sem dados. Use POST /reload para tentar novamente.")
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

# CORS — permite acesso do Lovable e qualquer frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, restrinja ao domínio do Lovable
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================
# HELPERS
# ============================================================

def df_to_response(df, limit: int = 1000, offset: int = 0) -> dict:
    """Converte DataFrame para resposta JSON com paginação."""
    if df is None or df.empty:
        return {"data": [], "total": 0, "limit": limit, "offset": offset}

    total = len(df)
    page = df.iloc[offset : offset + limit]

    # Converte datas para string
    for col in page.columns:
        if hasattr(page[col], "dt"):
            try:
                page[col] = page[col].dt.strftime("%Y-%m-%d")
            except Exception:
                page[col] = page[col].astype(str)

    return {
        "data": page.to_dict(orient="records"),
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def check_loaded():
    """Verifica se os dados estão carregados."""
    if not data_service.loaded:
        raise HTTPException(
            status_code=503,
            detail="Dados ainda não carregados. Aguarde ou use POST /reload.",
        )


# ============================================================
# ENDPOINTS
# ============================================================

@app.get("/")
async def root():
    """Health check."""
    return {
        "service": "CVM Financials API",
        "status": "ok" if data_service.loaded else "loading",
        "docs": "/docs",
    }


@app.get("/status")
async def status():
    """Status detalhado do serviço."""
    return data_service.get_status()


@app.post("/reload")
async def reload(
    use_cache: bool = Query(True, description="Usar cache local"),
):
    """Recarrega dados da CVM."""
    try:
        data_service.load(use_cache=use_cache)
        return {"status": "ok", "message": "Dados recarregados com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- BUSCA POR TICKER ---

@app.get("/search")
async def search_ticker(
    q: str = Query(..., min_length=2, description="Ticker ou nome (ex: PETR4, Petrobras)"),
):
    """
    Busca empresas por ticker ou nome.

    Retorna ticker(s), código CVM, CNPJ e nome.
    Use o cd_cvm retornado para consultar os demonstrativos,
    ou passe o ticker diretamente nos outros endpoints.
    """
    check_loaded()
    results = data_service.search_ticker(q)
    return {"results": results, "total": len(results)}


# --- EMPRESA POR TICKER ---

@app.get("/ticker/{ticker}")
async def company_by_ticker(
    ticker: str,
):
    """
    Retorna todos os demonstrativos de uma empresa pelo ticker B3.

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


# --- EMPRESAS ---

@app.get("/empresas")
async def list_companies(
    search: str | None = Query(None, description="Busca por nome da empresa"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Lista empresas disponíveis."""
    check_loaded()
    df = data_service.get_companies(search=search)
    return df_to_response(df, limit, offset)


@app.get("/empresa/{cd_cvm}")
async def company_financials(
    cd_cvm: str,
):
    """
    Retorna todos os demonstrativos de uma empresa pelo código CVM.

    Parâmetro: cd_cvm = Código CVM da empresa.
    Dica: use /ticker/{ticker} para buscar por ticker B3.
    """
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
    cnpj: str | None = Query(None, description="CNPJ da empresa"),
    cd_cvm: str | None = Query(None, description="Código CVM"),
    dt_refer: str | None = Query(None, description="Data referência (YYYY-MM-DD)"),
    raw: bool = Query(False, description="Se True, retorna dados não-pivotados"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """
    Demonstração do Resultado do Exercício.

    Filtros opcionais: ticker, cnpj, cd_cvm, dt_refer.
    Exemplo: /dre?ticker=PETR4
    """
    check_loaded()
    df = data_service.get_statement(
        "DRE", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


# --- BALANÇO PATRIMONIAL ---

@app.get("/balanco/ativo")
async def get_bpa(
    ticker: str | None = Query(None, description="Ticker B3 (ex: PETR4)"),
    cnpj: str | None = Query(None),
    cd_cvm: str | None = Query(None),
    dt_refer: str | None = Query(None),
    raw: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Balanço Patrimonial — Ativo. Exemplo: /balanco/ativo?ticker=VALE3"""
    check_loaded()
    df = data_service.get_statement(
        "BPA", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


@app.get("/balanco/passivo")
async def get_bpp(
    ticker: str | None = Query(None, description="Ticker B3 (ex: PETR4)"),
    cnpj: str | None = Query(None),
    cd_cvm: str | None = Query(None),
    dt_refer: str | None = Query(None),
    raw: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Balanço Patrimonial — Passivo e PL. Exemplo: /balanco/passivo?ticker=ITUB4"""
    check_loaded()
    df = data_service.get_statement(
        "BPP", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


# --- DFC ---

@app.get("/dfc")
async def get_dfc(
    ticker: str | None = Query(None, description="Ticker B3 (ex: PETR4)"),
    cnpj: str | None = Query(None),
    cd_cvm: str | None = Query(None),
    dt_refer: str | None = Query(None),
    raw: bool = Query(False),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """Demonstração do Fluxo de Caixa. Exemplo: /dfc?ticker=BBAS3"""
    check_loaded()
    df = data_service.get_statement(
        "DFC", cnpj=cnpj, cd_cvm=cd_cvm, ticker=ticker,
        dt_refer=dt_refer, pivoted=not raw,
    )
    return df_to_response(df, limit, offset)


# --- CONTAS ---

@app.get("/contas/{statement}")
async def available_accounts(
    statement: str,
):
    """
    Lista contas rastreadas para um demonstrativo.

    Parâmetros: statement = DRE | BPA | BPP | DFC_MI | DFC_MD
    """
    valid = ["DRE", "BPA", "BPP", "DFC_MI", "DFC_MD"]
    if statement.upper() not in valid:
        raise HTTPException(
            status_code=400,
            detail=f"Demonstrativo inválido. Use: {valid}",
        )
    return data_service.get_available_accounts(statement.upper())
