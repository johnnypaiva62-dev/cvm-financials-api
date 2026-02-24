"""
Módulo para parsing e limpeza dos dados da CVM.

Transforma os CSVs brutos em DataFrames estruturados,
com contas padronizadas e valores numéricos.

Colunas originais dos CSVs da CVM:
    CNPJ_CIA         - CNPJ da empresa
    DENOM_CIA        - Nome da empresa
    CD_CVM           - Código CVM
    DT_REFER         - Data de referência (YYYY-MM-DD)
    DT_INI_EXERC     - Início do exercício
    DT_FIM_EXERC     - Fim do exercício
    ORDEM_EXERC      - "ÚLTIMO" ou "PENÚLTIMO"
    CD_CONTA         - Código da conta (1, 1.01, 1.01.01, etc.)
    DS_CONTA         - Descrição da conta
    VL_CONTA         - Valor (string, separador decimal = vírgula)
    ST_CONTA_FIXA    - Se é conta fixa/padrão (S/N)
    COLUNA_DF        - Tipo de coluna da demonstração
    ESCALA_MOEDA     - Escala (MIL, UNIDADE)
    MOEDA            - Moeda (REAL)
"""

import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# ============================================================
# CONTAS PRINCIPAIS POR DEMONSTRATIVO
# ============================================================
# Mapeamento dos códigos de conta mais relevantes.
# Você pode expandir conforme necessidade.

# DRE - Demonstração do Resultado do Exercício
DRE_ACCOUNTS = {
    "3.01": "Receita Líquida",
    "3.02": "Custo dos Bens e/ou Serviços Vendidos",
    "3.03": "Resultado Bruto",
    "3.04": "Despesas/Receitas Operacionais",
    "3.05": "Resultado Antes do Resultado Financeiro e dos Tributos",
    "3.06": "Resultado Financeiro",
    "3.06.01": "Receitas Financeiras",
    "3.06.02": "Despesas Financeiras",
    "3.07": "Resultado Antes dos Tributos sobre o Lucro",
    "3.08": "Imposto de Renda e Contribuição Social",
    "3.09": "Resultado Líquido das Operações Continuadas",
    "3.11": "Lucro/Prejuízo do Período",
}

# BPA - Balanço Patrimonial Ativo
BPA_ACCOUNTS = {
    "1": "Ativo Total",
    "1.01": "Ativo Circulante",
    "1.01.01": "Caixa e Equivalentes de Caixa",
    "1.01.02": "Aplicações Financeiras",
    "1.01.03": "Contas a Receber",
    "1.01.04": "Estoques",
    "1.02": "Ativo Não Circulante",
    "1.02.01": "Ativo Realizável a Longo Prazo",
    "1.02.02": "Investimentos",
    "1.02.03": "Imobilizado",
    "1.02.04": "Intangível",
}

# BPP - Balanço Patrimonial Passivo
BPP_ACCOUNTS = {
    "2": "Passivo Total",
    "2.01": "Passivo Circulante",
    "2.01.04": "Empréstimos e Financiamentos CP",
    "2.02": "Passivo Não Circulante",
    "2.02.01": "Empréstimos e Financiamentos LP",
    "2.03": "Patrimônio Líquido Consolidado",
    "2.03.01": "Capital Social Realizado",
    "2.03.04": "Reservas de Lucros",
    "2.03.08": "Outros Resultados Abrangentes",
}

# DFC - Demonstração do Fluxo de Caixa
DFC_ACCOUNTS = {
    "6.01": "Caixa Líquido Atividades Operacionais",
    "6.02": "Caixa Líquido Atividades de Investimento",
    "6.03": "Caixa Líquido Atividades de Financiamento",
    "6.05": "Aumento (Redução) de Caixa e Equivalentes",
}

ACCOUNTS_MAP = {
    "DRE": DRE_ACCOUNTS,
    "BPA": BPA_ACCOUNTS,
    "BPP": BPP_ACCOUNTS,
    "DFC_MI": DFC_ACCOUNTS,
    "DFC_MD": DFC_ACCOUNTS,
}


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa e padroniza um DataFrame bruto da CVM.

    - Converte VL_CONTA para numérico
    - Normaliza escala (MIL → multiplica por 1000)
    - Converte datas
    - Remove duplicatas
    """
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Remove espaços dos nomes de colunas
    df.columns = [c.strip() for c in df.columns]

    # Converte valor para numérico
    if "VL_CONTA" in df.columns:
        df["VL_CONTA"] = (
            df["VL_CONTA"]
            .astype(str)
            .str.replace(",", ".", regex=False)
            .str.strip()
        )
        df["VL_CONTA"] = pd.to_numeric(df["VL_CONTA"], errors="coerce")

        # Ajusta escala: se está em MIL, multiplica por 1000
        if "ESCALA_MOEDA" in df.columns:
            mask_mil = df["ESCALA_MOEDA"].str.strip().str.upper() == "MIL"
            df.loc[mask_mil, "VL_CONTA"] = df.loc[mask_mil, "VL_CONTA"] * 1000

    # Converte datas
    for col in ["DT_REFER", "DT_INI_EXERC", "DT_FIM_EXERC"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Filtra apenas ÚLTIMO exercício (remove reapresentações)
    if "ORDEM_EXERC" in df.columns:
        df = df[df["ORDEM_EXERC"].str.strip().str.upper() == "ÚLTIMO"]

    # Remove duplicatas por empresa + data + conta
    key_cols = ["CNPJ_CIA", "DT_REFER", "CD_CONTA"]
    existing_keys = [c for c in key_cols if c in df.columns]
    if existing_keys:
        df = df.drop_duplicates(subset=existing_keys, keep="last")

    return df


def filter_main_accounts(
    df: pd.DataFrame,
    statement_type: str,
) -> pd.DataFrame:
    """
    Filtra apenas as contas principais de um demonstrativo.

    Args:
        df: DataFrame limpo
        statement_type: "DRE", "BPA", "BPP", "DFC_MI", "DFC_MD"

    Returns:
        DataFrame filtrado com as contas mais relevantes
    """
    accounts = ACCOUNTS_MAP.get(statement_type, {})
    if not accounts:
        return df

    # Filtra pelas contas de interesse
    mask = df["CD_CONTA"].isin(accounts.keys())
    filtered = df[mask].copy()

    # Adiciona nome amigável da conta
    filtered["CONTA_NOME"] = filtered["CD_CONTA"].map(accounts)

    return filtered


def pivot_statement(
    df: pd.DataFrame,
    statement_type: str,
) -> pd.DataFrame:
    """
    Pivota um demonstrativo para formato tabular.

    De:
        CNPJ | DT_REFER | CD_CONTA | DS_CONTA | VL_CONTA
    Para:
        CNPJ | DENOM_CIA | DT_REFER | Receita Líquida | Lucro Líquido | ...
    """
    if df.empty:
        return df

    accounts = ACCOUNTS_MAP.get(statement_type, {})

    # Filtra contas principais
    filtered = df[df["CD_CONTA"].isin(accounts.keys())].copy()
    filtered["CONTA_LABEL"] = filtered["CD_CONTA"].map(accounts)

    # Pivot
    pivoted = filtered.pivot_table(
        index=["CNPJ_CIA", "DENOM_CIA", "CD_CVM", "DT_REFER"],
        columns="CONTA_LABEL",
        values="VL_CONTA",
        aggfunc="first",
    ).reset_index()

    # Achata MultiIndex das colunas
    if isinstance(pivoted.columns, pd.MultiIndex):
        pivoted.columns = [
            col[1] if col[1] else col[0] for col in pivoted.columns
        ]

    return pivoted


def build_company_list(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Monta lista de empresas a partir dos dados baixados.

    Returns:
        DataFrame: CNPJ_CIA | DENOM_CIA | CD_CVM (únicos)
    """
    all_companies = []

    for statement_type, df in data.items():
        if df is not None and not df.empty:
            cols = ["CNPJ_CIA", "DENOM_CIA", "CD_CVM"]
            existing = [c for c in cols if c in df.columns]
            companies = df[existing].drop_duplicates()
            all_companies.append(companies)

    if not all_companies:
        return pd.DataFrame(columns=["CNPJ_CIA", "DENOM_CIA", "CD_CVM"])

    result = pd.concat(all_companies, ignore_index=True).drop_duplicates(
        subset=["CNPJ_CIA"], keep="first"
    )
    return result.sort_values("DENOM_CIA").reset_index(drop=True)


def process_all_statements(
    raw_data: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """
    Processa todos os demonstrativos: limpa, filtra, pivota.

    Args:
        raw_data: Dict de {statement_type: raw_df} do downloader

    Returns:
        Dict com DataFrames processados:
        {
            "DRE": df_dre_pivotado,
            "BPA": df_bpa_pivotado,
            "BPP": df_bpp_pivotado,
            "DFC": df_dfc_pivotado,  # MI + MD combinados
            "empresas": df_empresas,
            "DRE_raw": df_dre_limpo,
            ...
        }
    """
    result = {}

    # Processa cada demonstrativo
    for statement in ["DRE", "BPA", "BPP"]:
        if statement in raw_data:
            logger.info(f"Processando {statement}...")
            cleaned = clean_dataframe(raw_data[statement])
            result[f"{statement}_raw"] = cleaned
            result[statement] = pivot_statement(cleaned, statement)
            logger.info(f"  {statement}: {len(result[statement])} registros pivotados")

    # DFC: combina Método Indireto e Direto (MI é mais comum)
    dfc_df = None
    if "DFC_MI" in raw_data:
        dfc_df = clean_dataframe(raw_data["DFC_MI"])
    if "DFC_MD" in raw_data:
        dfc_md = clean_dataframe(raw_data["DFC_MD"])
        if dfc_df is not None:
            # Adiciona empresas do método direto que não estão no indireto
            existing_keys = set(
                zip(dfc_df["CNPJ_CIA"], dfc_df["DT_REFER"].astype(str))
            )
            new_rows = dfc_md[
                ~dfc_md.apply(
                    lambda r: (r["CNPJ_CIA"], str(r["DT_REFER"])) in existing_keys,
                    axis=1,
                )
            ]
            dfc_df = pd.concat([dfc_df, new_rows], ignore_index=True)
        else:
            dfc_df = dfc_md

    if dfc_df is not None and not dfc_df.empty:
        result["DFC_raw"] = dfc_df
        result["DFC"] = pivot_statement(dfc_df, "DFC_MI")
        logger.info(f"  DFC: {len(result['DFC'])} registros pivotados")

    # Lista de empresas
    result["empresas"] = build_company_list(raw_data)
    logger.info(f"  Empresas: {len(result['empresas'])}")

    return result
