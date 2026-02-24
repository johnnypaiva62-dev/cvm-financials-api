"""
Módulo para cálculo de indicadores financeiros a partir dos dados da CVM.

Computa:
    - Margens (Bruta, EBIT, Pré-Imposto, Líquida, FCF)
    - Retornos (ROA, ROE, ROIC)
    - Saúde Financeira (Caixa, Dívida, D/E, Cobertura de Juros)
    - Growth CAGRs (Receita, Lucro 3Y/5Y/10Y)
    - DFC (FCO, CAPEX, FCF, Dividendos)
"""

import math
import pandas as pd
import numpy as np

# Taxa de IR/CS brasileira para NOPAT
BRAZIL_TAX_RATE = 0.34


def _safe_div(a, b):
    """Divisão segura retornando None se inválido."""
    if b is None or a is None or b == 0 or math.isnan(a) or math.isnan(b):
        return None
    return a / b


def _safe_pct(a, b):
    """Percentual seguro."""
    r = _safe_div(a, b)
    return round(r * 100, 2) if r is not None else None


def _safe_round(v, n=2):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return round(v, n)


def _get(row, key, default=None):
    """Pega valor de um dict/Series, retorna default se NaN."""
    v = row.get(key, default) if isinstance(row, dict) else getattr(row, key, default)
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return default
    return v


def _cagr(values, years):
    """
    Calcula CAGR entre o primeiro e último valor de uma série.
    values: lista de (year, value) ordenada por year.
    years: período desejado (3, 5, 10).
    """
    if not values or len(values) < 2:
        return None

    # Pega último valor
    end_year, end_val = values[-1]
    if end_val is None or end_val == 0:
        return None

    # Encontra valor mais próximo de N anos atrás
    target_year = end_year - years
    start = None
    for y, v in values:
        if v is not None and v != 0:
            if y <= target_year:
                start = (y, v)
            elif start is None and y > target_year:
                # Se não tem exatamente N anos, usa o mais antigo disponível
                break

    if start is None:
        # Tenta usar o valor mais antigo disponível
        for y, v in values:
            if v is not None and v != 0:
                start = (y, v)
                break

    if start is None or start[0] == end_year:
        return None

    n = end_year - start[0]
    if n <= 0:
        return None

    start_val = start[1]
    # CAGR não faz sentido com valores negativos
    if start_val <= 0 or end_val <= 0:
        return None

    cagr = (end_val / start_val) ** (1 / n) - 1
    return round(cagr * 100, 2)


def compute_overview(dre_df, bpa_df, bpp_df, dfc_df, dre_annual_df=None):
    """
    Computa indicadores financeiros completos.

    Args:
        dre_df: DRE trimestral (pivotado)
        bpa_df: BPA (pivotado)
        bpp_df: BPP (pivotado)
        dfc_df: DFC (pivotado)
        dre_annual_df: DRE anual (pivotado) — para CAGRs

    Returns:
        Dict com todos os indicadores
    """
    result = {
        "company": {},
        "latest_period": None,
        "margins": {},
        "returns": {},
        "financial_health": {},
        "cash_flow": {},
        "growth": {},
        "history": {},
    }

    # ── Determina empresa e períodos ──
    all_dfs = [dre_df, bpa_df, bpp_df, dfc_df]
    for df in all_dfs:
        if df is not None and not df.empty and "DENOM_CIA" in df.columns:
            latest = df.iloc[-1]
            result["company"]["nome"] = _get(latest, "DENOM_CIA")
            result["company"]["cnpj"] = _get(latest, "CNPJ_CIA")
            result["company"]["cd_cvm"] = _get(latest, "CD_CVM")
            break

    # ── Usa dados anuais para cálculos (mais consistentes) ──
    # Se temos DRE anual, usa para métricas de performance
    use_dre = dre_annual_df if dre_annual_df is not None and not dre_annual_df.empty else dre_df

    if use_dre is None or use_dre.empty:
        return result

    # Dados mais recentes
    latest_dre = use_dre.iloc[-1].to_dict()
    result["latest_period"] = _get(latest_dre, "DT_REFER")

    # ── MARGENS (último período) ──
    receita = _get(latest_dre, "Receita Líquida", 0)
    resultado_bruto = _get(latest_dre, "Resultado Bruto")
    ebit = _get(latest_dre, "EBIT")
    ebt = _get(latest_dre, "EBT")
    lucro = _get(latest_dre, "Lucro/Prejuízo Consolidado do Período")

    result["margins"] = {
        "receita": _safe_round(receita, 0),
        "bruta": _safe_pct(resultado_bruto, receita),
        "ebit": _safe_pct(ebit, receita),
        "pre_imposto": _safe_pct(ebt, receita),
        "liquida": _safe_pct(lucro, receita),
    }

    # ── BALANÇO (último período) ──
    latest_bpa = bpa_df.iloc[-1].to_dict() if bpa_df is not None and not bpa_df.empty else {}
    latest_bpp = bpp_df.iloc[-1].to_dict() if bpp_df is not None and not bpp_df.empty else {}

    ativo_total = _get(latest_bpa, "Ativo Total")
    caixa = _get(latest_bpa, "Caixa e Equivalentes de Caixa", 0)
    aplic_fin = _get(latest_bpa, "Aplicações Financeiras", 0)
    caixa_total = (caixa or 0) + (aplic_fin or 0)

    emprestimos_cp = _get(latest_bpp, "Empréstimos e Financiamentos CP", 0)
    emprestimos_lp = _get(latest_bpp, "Empréstimos e Financiamentos LP", 0)
    divida_bruta = (emprestimos_cp or 0) + (emprestimos_lp or 0)
    divida_liquida = divida_bruta - caixa_total

    patrimonio_liq = _get(latest_bpp, "Patrimônio Líquido Consolidado")

    desp_financeiras = abs(_get(latest_dre, "Despesas Financeiras", 0) or 0)

    result["financial_health"] = {
        "ativo_total": _safe_round(ativo_total, 0),
        "caixa": _safe_round(caixa_total, 0),
        "divida_bruta": _safe_round(divida_bruta, 0),
        "divida_liquida": _safe_round(divida_liquida, 0),
        "patrimonio_liquido": _safe_round(patrimonio_liq, 0),
        "debt_equity": _safe_round(_safe_div(divida_bruta, patrimonio_liq), 2),
        "divida_liq_ebitda": None,  # Precisaríamos de D&A para EBITDA
        "ebit_interest": _safe_round(_safe_div(ebit, desp_financeiras), 2) if desp_financeiras else None,
    }

    # ── RETORNOS ──
    result["returns"] = {
        "roa": _safe_pct(lucro, ativo_total),
        "roe": _safe_pct(lucro, patrimonio_liq),
        "roic": None,
    }

    # ROIC = NOPAT / Capital Investido
    if ebit and patrimonio_liq and divida_liquida is not None:
        nopat = ebit * (1 - BRAZIL_TAX_RATE)
        capital_investido = (patrimonio_liq or 0) + max(divida_liquida, 0)
        if capital_investido > 0:
            result["returns"]["roic"] = _safe_pct(nopat, capital_investido)

    # ── FLUXO DE CAIXA ──
    latest_dfc = dfc_df.iloc[-1].to_dict() if dfc_df is not None and not dfc_df.empty else {}

    fco = _get(latest_dfc, "Caixa Líquido Atividades Operacionais")
    capex_imob = _get(latest_dfc, "Aquisição de Imobilizado", 0)
    capex_intang = _get(latest_dfc, "Aquisição de Intangível", 0)
    capex = (capex_imob or 0) + (capex_intang or 0)  # Negativo
    fcf = (fco or 0) + capex if fco else None
    dividendos = _get(latest_dfc, "Pagamento de Dividendos")

    result["cash_flow"] = {
        "fco": _safe_round(fco, 0),
        "capex": _safe_round(capex, 0),
        "fcf": _safe_round(fcf, 0),
        "fcf_margin": _safe_pct(fcf, receita) if fcf and receita else None,
        "dividendos": _safe_round(dividendos, 0),
        "payout": _safe_pct(abs(dividendos or 0), lucro) if dividendos and lucro and lucro > 0 else None,
    }

    # ── GROWTH CAGRs (usa dados anuais) ──
    annual = dre_annual_df if dre_annual_df is not None and not dre_annual_df.empty else use_dre

    rev_history = []
    lucro_history = []
    ebit_history = []

    for _, row in annual.iterrows():
        d = row.to_dict()
        dt = _get(d, "DT_REFER")
        if not dt:
            continue
        y = int(str(dt)[:4])
        rev = _get(d, "Receita Líquida")
        luc = _get(d, "Lucro/Prejuízo Consolidado do Período")
        eb = _get(d, "EBIT")
        if rev:
            rev_history.append((y, rev))
        if luc:
            lucro_history.append((y, luc))
        if eb:
            ebit_history.append((y, eb))

    result["growth"] = {
        "receita_3y": _cagr(rev_history, 3),
        "receita_5y": _cagr(rev_history, 5),
        "receita_10y": _cagr(rev_history, 10),
        "lucro_3y": _cagr(lucro_history, 3),
        "lucro_5y": _cagr(lucro_history, 5),
        "lucro_10y": _cagr(lucro_history, 10),
        "ebit_3y": _cagr(ebit_history, 3),
        "ebit_5y": _cagr(ebit_history, 5),
        "ebit_10y": _cagr(ebit_history, 10),
    }

    # ── HISTORY (para sparklines / mini charts) ──
    hist_receita = []
    hist_lucro = []
    hist_margem = []

    for _, row in annual.iterrows():
        d = row.to_dict()
        dt = _get(d, "DT_REFER")
        rev = _get(d, "Receita Líquida")
        luc = _get(d, "Lucro/Prejuízo Consolidado do Período")
        if dt and rev:
            hist_receita.append({"dt": str(dt)[:10], "value": _safe_round(rev, 0)})
        if dt and luc:
            hist_lucro.append({"dt": str(dt)[:10], "value": _safe_round(luc, 0)})
        if dt and rev and luc:
            hist_margem.append({"dt": str(dt)[:10], "value": _safe_round((luc / rev) * 100, 2)})

    result["history"] = {
        "receita": hist_receita,
        "lucro": hist_lucro,
        "margem_liquida": hist_margem,
    }

    return result
