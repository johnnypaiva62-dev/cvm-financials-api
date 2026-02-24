"""
Serviço de dados — orquestra download, parse e cache.

Mantém os dados em memória após o primeiro carregamento
e oferece interface simples para a API consultar.
"""

import logging
from datetime import datetime

import pandas as pd

from app.downloader import download_multiple_years
from app.parser import process_all_statements, clean_dataframe, ACCOUNTS_MAP
from app.ticker_mapper import ticker_mapper

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURAÇÃO DE ANOS
# ============================================================
# CVM disponibiliza dados desde 2010.
# Mais anos = mais download (~50-100MB por ano), mas cobre
# o maior histórico possível.
DEFAULT_ITR_YEARS = list(range(2010, 2026))  # 2010 a 2025
DEFAULT_DFP_YEARS = list(range(2010, 2025))  # 2010 a 2024


class DataService:
    """
    Serviço central de dados.

    Baixa, processa e mantém em memória os dados da CVM.
    """

    def __init__(self):
        self.data: dict[str, pd.DataFrame] = {}
        self.last_update: datetime | None = None
        self.loaded = False

    def load(
        self,
        itr_years: list[int] | None = None,
        dfp_years: list[int] | None = None,
        use_cache: bool = True,
    ):
        """
        Carrega todos os dados da CVM.

        Baixa ITR (trimestrais) e DFP (anuais), processa e armazena.
        """
        itr_years = itr_years or DEFAULT_ITR_YEARS
        dfp_years = dfp_years or DEFAULT_DFP_YEARS

        logger.info("=" * 60)
        logger.info("CARREGANDO DADOS DA CVM")
        logger.info("=" * 60)

        # 1. Download ITR (trimestrais)
        logger.info(f"\n--- ITR (Trimestrais): {itr_years} ---")
        raw_itr = download_multiple_years(itr_years, "itr", use_cache)

        # 2. Download DFP (anuais)
        logger.info(f"\n--- DFP (Anuais): {dfp_years} ---")
        raw_dfp = download_multiple_years(dfp_years, "dfp", use_cache)

        # 3. Combina ITR + DFP
        combined = {}
        for key in set(list(raw_itr.keys()) + list(raw_dfp.keys())):
            dfs = []
            if key in raw_itr:
                dfs.append(raw_itr[key])
            if key in raw_dfp:
                dfs.append(raw_dfp[key])
            combined[key] = pd.concat(dfs, ignore_index=True)

        # 4. Processa
        logger.info("\n--- Processando ---")
        self.data = process_all_statements(combined)

        # 5. Carrega mapeamento de tickers
        logger.info("\n--- Carregando mapeamento de tickers ---")
        ticker_mapper.load(use_cache=use_cache)

        # 6. Enriquece DataFrames com tickers
        for key in list(self.data.keys()):
            df = self.data[key]
            if isinstance(df, pd.DataFrame) and "CNPJ_CIA" in df.columns:
                self.data[key] = ticker_mapper.enrich_dataframe(df)

        self.last_update = datetime.now()
        self.loaded = True

        logger.info(f"\n✓ Dados carregados: {list(self.data.keys())}")
        logger.info(f"✓ Última atualização: {self.last_update}")

    # ========================================================
    # CONSULTAS
    # ========================================================

    def get_companies(self, search: str | None = None) -> pd.DataFrame:
        """Lista de empresas. Filtra por nome se `search` fornecido."""
        df = self.data.get("empresas", pd.DataFrame())
        if search and not df.empty:
            mask = df["DENOM_CIA"].str.contains(
                search, case=False, na=False
            )
            df = df[mask]
        return df

    def get_statement(
        self,
        statement: str,
        cnpj: str | None = None,
        cd_cvm: str | None = None,
        ticker: str | None = None,
        dt_refer: str | None = None,
        pivoted: bool = True,
    ) -> pd.DataFrame:
        """
        Retorna um demonstrativo.

        Args:
            statement: "DRE", "BPA", "BPP", "DFC"
            cnpj: Filtro por CNPJ
            cd_cvm: Filtro por código CVM
            ticker: Filtro por ticker B3 (ex: PETR4)
            dt_refer: Filtro por data de referência (YYYY-MM-DD)
            pivoted: Se True, retorna pivotado; se False, retorna raw

        Returns:
            DataFrame filtrado
        """
        # Resolve ticker → cd_cvm se necessário
        if ticker and not cd_cvm and not cnpj:
            cd_cvm = ticker_mapper.ticker_to_cvm(ticker)
            if not cd_cvm:
                cnpj = ticker_mapper.ticker_to_cnpj(ticker)

        key = statement if pivoted else f"{statement}_raw"
        df = self.data.get(key, pd.DataFrame())

        if df.empty:
            return df

        # Aplica filtros
        if cnpj:
            cnpj_clean = cnpj.replace(".", "").replace("/", "").replace("-", "")
            df = df[
                df["CNPJ_CIA"].astype(str).str.replace(r"[.\-/]", "", regex=True) == cnpj_clean
            ]

        if cd_cvm:
            # Normaliza removendo zeros à esquerda (CVM usa "004170", mapper retorna "4170")
            cd_cvm_str = str(cd_cvm).lstrip("0") or "0"
            df = df[df["CD_CVM"].astype(str).str.lstrip("0") == cd_cvm_str]

        if dt_refer:
            if "DT_REFER" in df.columns:
                df = df[df["DT_REFER"].astype(str).str[:10] == dt_refer]

        return df

    def get_company_financials(
        self,
        cnpj: str | None = None,
        cd_cvm: str | None = None,
        ticker: str | None = None,
    ) -> dict:
        """
        Retorna todos os demonstrativos de uma empresa.

        Args:
            cnpj: CNPJ da empresa
            cd_cvm: Código CVM
            ticker: Ticker B3 (ex: PETR4)

        Returns:
            Dict com DRE, BPA, BPP, DFC da empresa
        """
        # Resolve ticker
        if ticker and not cd_cvm and not cnpj:
            cd_cvm = ticker_mapper.ticker_to_cvm(ticker)
            if not cd_cvm:
                cnpj = ticker_mapper.ticker_to_cnpj(ticker)

        result = {}
        for stmt in ["DRE", "BPA", "BPP", "DFC"]:
            df = self.get_statement(stmt, cnpj=cnpj, cd_cvm=cd_cvm)
            if not df.empty:
                # Converte datas para string para serialização JSON
                for col in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        df[col] = df[col].dt.strftime("%Y-%m-%d")
                result[stmt] = df.to_dict(orient="records")
            else:
                result[stmt] = []

        return result

    def search_ticker(self, query: str) -> list[dict]:
        """Busca por ticker ou nome de empresa."""
        return ticker_mapper.search_ticker(query)

    def get_available_accounts(self, statement: str) -> list[dict]:
        """
        Retorna as contas disponíveis para um demonstrativo.
        """
        accounts = ACCOUNTS_MAP.get(statement, {})
        return [
            {"codigo": k, "descricao": v}
            for k, v in accounts.items()
        ]

    def get_status(self) -> dict:
        """Retorna status do serviço."""
        counts = {}
        for key, df in self.data.items():
            if isinstance(df, pd.DataFrame):
                counts[key] = len(df)

        return {
            "loaded": self.loaded,
            "last_update": (
                self.last_update.isoformat() if self.last_update else None
            ),
            "tables": counts,
        }


# Singleton
data_service = DataService()
