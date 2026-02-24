"""
Módulo de mapeamento Ticker B3 ↔ Código CVM / CNPJ.

Os dados da CVM não contêm ticker de negociação.
Este módulo faz o de-para usando:
1. Cadastro de cias abertas da CVM (CD_CVM ↔ CNPJ ↔ Nome)
2. Mapeamento estático ticker ↔ CNPJ (principais empresas)
3. Busca fuzzy por nome como fallback

Fonte do cadastro:
    https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv
"""

import io
import logging
from pathlib import Path

import requests
import pandas as pd

logger = logging.getLogger(__name__)

CAD_URL = "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
CACHE_DIR = Path("data/cache")


def download_cad_cia_aberta(use_cache: bool = True) -> pd.DataFrame:
    """
    Baixa o cadastro de companhias abertas da CVM.

    Contém: CD_CVM, CNPJ_CIA, DENOM_CIA, DENOM_ANTERIOR,
            SIT, DT_REG, DT_CANCEL, etc.
    """
    cache_path = CACHE_DIR / "cad_cia_aberta.csv"

    if use_cache and cache_path.exists():
        logger.info("Usando cache do cadastro CVM")
        df = pd.read_csv(cache_path, sep=";", encoding="latin-1", dtype=str)
    else:
        logger.info(f"Baixando cadastro CVM: {CAD_URL}")
        resp = requests.get(CAD_URL, timeout=60)
        resp.raise_for_status()

        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(resp.content)

        df = pd.read_csv(
            io.BytesIO(resp.content), sep=";", encoding="latin-1", dtype=str
        )

    # Limpa nomes de colunas
    df.columns = [c.strip().replace("\ufeff", "") for c in df.columns]

    # Filtra apenas empresas ativas (SIT = ATIVO)
    if "SIT" in df.columns:
        df = df[df["SIT"].str.strip().str.upper() == "ATIVO"]

    return df


# ============================================================
# MAPEAMENTO TICKER → CNPJ
# ============================================================
# Mapeamento das principais empresas da B3.
# Formato: "TICKER": "CNPJ (só números)"
#
# Este mapeamento cobre ~150 empresas mais líquidas.
# Para empresas não listadas aqui, a busca por nome funciona
# como fallback.

TICKER_TO_CNPJ = {
    # Petróleo e Gás
    "PETR3": "33000167000101", "PETR4": "33000167000101",
    "PRIO3": "10629105000168", "RECV3": "02140597000102",
    "RRRP3": "02140597000102", "CSAN3": "50746577000115",
    "UGPA3": "02532132000173", "VBBR3": "01838723000127",

    # Mineração e Siderurgia
    "VALE3": "33592510000154",
    "GGBR3": "07358761000169", "GGBR4": "07358761000169",
    "GOAU3": "92690783000109", "GOAU4": "92690783000109",
    "CSNA3": "33042730000104", "USIM3": "60894730000105",
    "USIM5": "60894730000105", "CBAV3": "44597052000162",

    # Bancos
    "ITUB3": "60872504000123", "ITUB4": "60872504000123",
    "BBDC3": "60746948000112", "BBDC4": "60746948000112",
    "BBAS3": "00000000000191", "SANB11": "90400888000142",
    "BBSE3": "01522368000182", "BPAC11": "30306294000145",
    "ITSA3": "61532644000115", "ITSA4": "61532644000115",

    # Seguros e Financeiras
    "B3SA3": "09346601000125", "CIEL3": "01027058000191",
    "SUZB3": "16404287000155",

    # Energia
    "ELET3": "00001180000126", "ELET6": "00001180000126",
    "EQTL3": "03983431000103", "ENBR3": "03983431000103",
    "ENGI11": "07628438000184", "CMIG3": "17155730000164",
    "CMIG4": "17155730000164", "CPFE3": "02429144000193",
    "TAEE11": "07859971000130", "ENEV3": "04423567000121",
    "AURE3": "61695227000193", "CPLE3": "76483817000120",
    "CPLE6": "76483817000120", "NEOE3": "01083200000118",
    "AESB3": "02998609000127",

    # Varejo
    "MGLU3": "47960950000121", "VIIA3": "33041260065290",
    "LREN3": "92754738000162", "ARZZ3": "16590234000176",
    "SOMA3": "05765685000189", "PETZ3": "18328118000109",
    "ALPA3": "61079117000105", "ALPA4": "61079117000105",
    "BHIA3": "33041260065290", "CEAB3": "62782735000170",
    "GUAR3": "56116442000136",

    # Alimentos e Bebidas
    "ABEV3": "07526557000100", "JBSS3": "02916265000160",
    "BRFS3": "01838723000127", "MRFG3": "03853896000169",
    "BEEF3": "02916265000160", "MDIA3": "07206816000115",
    "SMTO3": "08424178000171", "CAML3": "64904295000103",

    # Saúde
    "RADL3": "45787678000102", "HAPV3": "63554067000198",
    "RDOR3": "06047087000139", "FLRY3": "60840055000131",
    "QUAL3": "11596594000155", "HYPE3": "02932074000191",
    "DXCO3": "97837181000147",

    # Imobiliário e Construção
    "CYRE3": "73178600000118", "MRVE3": "08769451000108",
    "EZTC3": "02149205000169", "EVEN3": "43470988000165",
    "TEND3": "71476527000135", "DIRR3": "16614075000100",
    "LAVV3": "25600126000109",

    # Logística e Transporte
    "RAIL3": "02387241000160", "CCRO3": "02846056000197",
    "ECOR3": "04149454000180", "AZUL4": "09296295000160",
    "GOLL4": "06164253000187", "EMBR3": "07689002000189",
    "STBP3": "02762121000104",

    # Telecomunicações
    "VIVT3": "02558157000162", "TIMS3": "02421421000111",
    "OIBR3": "76535764000143",

    # Papel e Celulose
    "KLBN11": "89637490000145", "KLBN3": "89637490000145",
    "KLBN4": "89637490000145",

    # Tecnologia
    "TOTS3": "53113791000122", "LWSA3": "13227137000150",
    "POSI3": "81243735000148", "CASH3": "28326000000160",
    "INTB3": "09129035000140",

    # Educação
    "COGN3": "02800026000140", "YDUQ3": "08807432000110",
    "ANIM3": "09288252000132",

    # Seguros
    "PSSA3": "02149205000169", "SULA11": "29978814000187",
    "IRBR3": "33376989000155",

    # Utilities e Saneamento
    "SBSP3": "43776517000180", "CSMG3": "17281106000103",

    # Shoppings e Properties
    "MULT3": "07816890000153", "IGTI11": "08070508000178",
    "BRML3": "06977745000102", "ALSO3": "05878397000132",

    # Indústria
    "WEGE3": "84429695000111", "RENT3": "16670085000155",
    "RAIZ4": "33453598000123", "MOVI3": "21314559000166",
    "VAMO3": "44155441000102", "NTCO3": "71673990000177",
    "LEVE3": "61249876000145", "TUPY3": "84683374000149",
    "FRAS3": "85437257000149",
}

# Mapeamento inverso: CNPJ → lista de tickers
CNPJ_TO_TICKERS: dict[str, list[str]] = {}
for _ticker, _cnpj in TICKER_TO_CNPJ.items():
    CNPJ_TO_TICKERS.setdefault(_cnpj, []).append(_ticker)


class TickerMapper:
    """
    Mapeia tickers da B3 para códigos CVM e vice-versa.
    """

    def __init__(self):
        self.cad: pd.DataFrame = pd.DataFrame()
        self._cnpj_to_cvm: dict[str, str] = {}
        self._cvm_to_cnpj: dict[str, str] = {}
        self._cvm_to_name: dict[str, str] = {}
        self.loaded = False

    def load(self, use_cache: bool = True):
        """Carrega cadastro CVM e monta índices."""
        self.cad = download_cad_cia_aberta(use_cache)

        for _, row in self.cad.iterrows():
            cnpj = (
                str(row.get("CNPJ_CIA", ""))
                .replace(".", "")
                .replace("/", "")
                .replace("-", "")
                .strip()
            )
            cd_cvm = str(row.get("CD_CVM", "")).strip()
            nome = str(row.get("DENOM_CIA", "")).strip()

            if cnpj and cd_cvm:
                self._cnpj_to_cvm[cnpj] = cd_cvm
                self._cvm_to_cnpj[cd_cvm] = cnpj
                self._cvm_to_name[cd_cvm] = nome

        self.loaded = True
        logger.info(f"TickerMapper: {len(self._cnpj_to_cvm)} empresas indexadas")

    def ticker_to_cvm(self, ticker: str) -> str | None:
        """Converte ticker (ex: PETR4) para código CVM."""
        ticker = ticker.upper().strip()
        cnpj = TICKER_TO_CNPJ.get(ticker)
        if cnpj:
            return self._cnpj_to_cvm.get(cnpj)
        return None

    def ticker_to_cnpj(self, ticker: str) -> str | None:
        """Converte ticker para CNPJ."""
        return TICKER_TO_CNPJ.get(ticker.upper().strip())

    def cvm_to_tickers(self, cd_cvm: str) -> list[str]:
        """Retorna tickers associados a um código CVM."""
        cnpj = self._cvm_to_cnpj.get(str(cd_cvm).strip())
        if cnpj:
            return CNPJ_TO_TICKERS.get(cnpj, [])
        return []

    def cvm_to_name(self, cd_cvm: str) -> str | None:
        """Retorna nome da empresa pelo código CVM."""
        return self._cvm_to_name.get(str(cd_cvm).strip())

    def search_ticker(self, query: str) -> list[dict]:
        """
        Busca tickers por nome parcial ou ticker.

        Retorna lista de matches com ticker, nome, cd_cvm, cnpj.
        """
        query = query.upper().strip()
        results = []

        # 1. Match exato por ticker
        if query in TICKER_TO_CNPJ:
            cnpj = TICKER_TO_CNPJ[query]
            cd_cvm = self._cnpj_to_cvm.get(cnpj)
            nome = self._cvm_to_name.get(cd_cvm, "") if cd_cvm else ""
            results.append({
                "ticker": query,
                "tickers_all": CNPJ_TO_TICKERS.get(cnpj, [query]),
                "cd_cvm": cd_cvm,
                "cnpj": cnpj,
                "nome": nome,
            })
            return results

        # 2. Match parcial por ticker (ex: "PETR" → PETR3, PETR4)
        ticker_matches = [
            t for t in TICKER_TO_CNPJ if t.startswith(query)
        ]
        seen_cnpjs = set()
        for t in ticker_matches:
            cnpj = TICKER_TO_CNPJ[t]
            if cnpj in seen_cnpjs:
                continue
            seen_cnpjs.add(cnpj)
            cd_cvm = self._cnpj_to_cvm.get(cnpj)
            nome = self._cvm_to_name.get(cd_cvm, "") if cd_cvm else ""
            results.append({
                "ticker": t,
                "tickers_all": CNPJ_TO_TICKERS.get(cnpj, [t]),
                "cd_cvm": cd_cvm,
                "cnpj": cnpj,
                "nome": nome,
            })

        if results:
            return results

        # 3. Busca por nome na base CVM
        if not self.cad.empty:
            mask = self.cad["DENOM_CIA"].str.upper().str.contains(query, na=False)
            for _, row in self.cad[mask].head(20).iterrows():
                cnpj = (
                    str(row.get("CNPJ_CIA", ""))
                    .replace(".", "").replace("/", "").replace("-", "")
                )
                cd_cvm = str(row.get("CD_CVM", "")).strip()
                nome = str(row.get("DENOM_CIA", "")).strip()
                tickers = CNPJ_TO_TICKERS.get(cnpj, [])
                results.append({
                    "ticker": tickers[0] if tickers else None,
                    "tickers_all": tickers,
                    "cd_cvm": cd_cvm,
                    "cnpj": cnpj,
                    "nome": nome,
                })

        return results

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adiciona coluna 'TICKERS' a um DataFrame que tenha CNPJ_CIA.
        """
        if df.empty or "CNPJ_CIA" not in df.columns:
            return df

        df = df.copy()
        df["TICKERS"] = df["CNPJ_CIA"].apply(
            lambda x: ", ".join(
                CNPJ_TO_TICKERS.get(
                    str(x).replace(".", "").replace("/", "").replace("-", ""),
                    [],
                )
            )
        )
        return df


# Singleton
ticker_mapper = TickerMapper()
