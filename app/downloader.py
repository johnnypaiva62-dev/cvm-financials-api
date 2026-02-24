"""
Módulo para download e cache dos dados da CVM (Cias Abertas).

Fontes:
- ITR: Informações Trimestrais
  https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/
- DFP: Demonstrações Financeiras Padronizadas (anuais)
  https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
"""

import os
import io
import zipfile
import logging
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd

logger = logging.getLogger(__name__)

# ============================================================
# URLs base da CVM
# ============================================================
CVM_BASE = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC"
ITR_URL = f"{CVM_BASE}/ITR/DADOS"
DFP_URL = f"{CVM_BASE}/DFP/DADOS"

# Tipos de demonstração que nos interessam
# BPA = Balanço Patrimonial Ativo
# BPP = Balanço Patrimonial Passivo
# DRE = Demonstração do Resultado
# DFC_MI = Demonstração do Fluxo de Caixa (Método Indireto)
# DFC_MD = Demonstração do Fluxo de Caixa (Método Direto)
STATEMENT_TYPES = ["BPA", "BPP", "DRE", "DFC_MI", "DFC_MD"]

# Preferência: consolidado (con) sobre individual (ind)
CONSOLIDATION_PREF = "con"

CACHE_DIR = Path("data/cache")


def ensure_cache_dir():
    """Cria diretório de cache se não existir."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _download_zip(url: str) -> bytes:
    """Baixa um arquivo ZIP da CVM."""
    logger.info(f"Baixando: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    logger.info(f"  OK ({len(response.content) / 1024 / 1024:.1f} MB)")
    return response.content


def _extract_csv_from_zip(
    zip_bytes: bytes,
    doc_type: str,  # "itr" ou "dfp"
    statement: str,  # "BPA", "DRE", etc.
    year: int,
    consolidation: str = CONSOLIDATION_PREF,
) -> pd.DataFrame | None:
    """
    Extrai um CSV específico de dentro do ZIP da CVM.

    O padrão de nome é:
        {doc_type}_cia_aberta_{statement}_{consolidation}_{year}.csv
    Exemplo:
        itr_cia_aberta_DRE_con_2024.csv
    """
    target_name = f"{doc_type}_cia_aberta_{statement}_{consolidation}_{year}.csv"

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # Lista arquivos no ZIP
        names = zf.namelist()

        if target_name not in names:
            # Tenta sem consolidação (alguns arquivos não têm _con/_ind)
            alt_name = f"{doc_type}_cia_aberta_{statement}_{year}.csv"
            if alt_name in names:
                target_name = alt_name
            else:
                logger.warning(f"  Arquivo não encontrado no ZIP: {target_name}")
                logger.debug(f"  Arquivos disponíveis: {names}")
                return None

        logger.info(f"  Extraindo: {target_name}")
        with zf.open(target_name) as f:
            # Lida com BOM (byte order mark) comum em CSVs da CVM
            df = pd.read_csv(
                f,
                sep=";",
                encoding="latin-1",
                dtype=str,
            )
            # Remove BOM de nomes de colunas
            df.columns = [col.replace("\ufeff", "").strip() for col in df.columns]
            return df


def download_year_data(
    year: int,
    doc_type: str = "itr",  # "itr" ou "dfp"
    use_cache: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Baixa e extrai todos os demonstrativos de um ano.

    Args:
        year: Ano (ex: 2024)
        doc_type: "itr" (trimestral) ou "dfp" (anual)
        use_cache: Se True, usa cache local

    Returns:
        Dict com {statement_type: DataFrame}
        Exemplo: {"DRE": df_dre, "BPA": df_bpa, ...}
    """
    ensure_cache_dir()

    base_url = ITR_URL if doc_type == "itr" else DFP_URL
    zip_filename = f"{doc_type}_cia_aberta_{year}.zip"
    url = f"{base_url}/{zip_filename}"
    cache_path = CACHE_DIR / zip_filename

    # Verifica cache
    if use_cache and cache_path.exists():
        logger.info(f"Usando cache: {cache_path}")
        zip_bytes = cache_path.read_bytes()
    else:
        try:
            zip_bytes = _download_zip(url)
            # Salva no cache
            cache_path.write_bytes(zip_bytes)
        except requests.HTTPError as e:
            logger.error(f"Erro ao baixar {url}: {e}")
            return {}

    # Extrai cada tipo de demonstrativo
    result = {}
    for statement in STATEMENT_TYPES:
        df = _extract_csv_from_zip(zip_bytes, doc_type, statement, year)
        if df is not None and not df.empty:
            result[statement] = df

    return result


def download_multiple_years(
    years: list[int],
    doc_type: str = "itr",
    use_cache: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Baixa dados de múltiplos anos e concatena.

    Returns:
        Dict com {statement_type: DataFrame_concatenado}
    """
    combined = {}

    for year in years:
        logger.info(f"\n--- {doc_type.upper()} {year} ---")
        year_data = download_year_data(year, doc_type, use_cache)

        for statement, df in year_data.items():
            if statement in combined:
                combined[statement] = pd.concat(
                    [combined[statement], df], ignore_index=True
                )
            else:
                combined[statement] = df

    # Log resumo
    for statement, df in combined.items():
        logger.info(f"  {statement}: {len(df)} registros")

    return combined
