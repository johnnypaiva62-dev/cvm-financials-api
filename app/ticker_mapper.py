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
    "AALR3": "42771949000135",
    "ABCB4": "28195667000106",
    "ABEV3": "07526557000100",
    "ADMF3": "52270350000171",
    "AERI3": "12528708000107",
    "AESO3": "00194724000113",
    "AFLT3": "10338320000100",
    "AGRO3": "07628528000159",
    "AGXY3": "21240146000184",
    "AHEB3": "62002886000160",
    "ALLD3": "20247322000147",
    "ALOS3": "05878397000132",
    "ALPA3": "61079117000105",
    "ALPA4": "61079117000105",
    "ALPK3": "60537263000166",
    "ALUP11": "08364948000138",
    "AMAR3": "61189288000189",
    "AMBP3": "12648266000124",
    "AMER3": "00776574000156",
    "AMOB3": "35654688000108",
    "ANIM3": "09288252000132",
    "APTI4": "61156931000178",
    "ARML3": "00242184000104",
    "ARND3": "14127813000151",
    "ASAI3": "06057223000171",
    "ATED3": "23994857000170",
    "AUAU3": "53153938000108",
    "AURA33": "07857093000114",
    "AURE3": "28594234000123",
    "AVLL3": "16811931000100",
    "AXIA3": "00001180000126",
    "AZEV4": "61351532000168",
    "AZTE3": "52017473000103",
    "AZUL53": "09305994000129",
    "AZZA3": "16590234000176",
    "B3SA3": "09346601000125",
    "BALM4": "61374161000130",
    "BAUH4": "95426862000197",
    "BAZA3": "04902979000144",
    "BBAS3": "00000000000191",
    "BBDC3": "60746948000112",
    "BBDC4": "60746948000112",
    "BBML3": "01107327000120",
    "BBSE3": "17344597000194",
    "BDLL4": "60851615000153",
    "BEEF3": "67620377000114",
    "BEES3": "28127603000178",
    "BETP3": "02762124000130",
    "BGIP4": "13009717000146",
    "BHIA3": "33041260065290",
    "BIED3": "45987245000192",
    "BIOM3": "04752991000110",
    "BLAU3": "58430828000160",
    "BMEB4": "17184037000110",
    "BMGB4": "61186680000174",
    "BMIN4": "34169557000172",
    "BMKS3": "56992423000190",
    "BMOB3": "09042817000105",
    "BNBR3": "07237373000120",
    "BOBR4": "50564053000103",
    "BPAC11": "30306294000145",
    "BPAR3": "04913711000108",
    "BRAP4": "03847461000192",
    "BRAV3": "12091809000155",
    "BRBI11": "10739356000103",
    "BRKM5": "42150391000170",
    "BRQB3": "36542025000164",
    "BRSR6": "92702067000196",
    "BRST3": "04601397000128",
    "BSLI3": "00000208000100",
    "CALI3": "61022042000118",
    "CAMB3": "61088894000108",
    "CAML3": "64904295000103",
    "CASH3": "14110585000107",
    "CASN3": "82508433000117",
    "CATA3": "19526748000150",
    "CBAV3": "61409892000173",
    "CBEE3": "33050071000158",
    "CCTY3": "52805925000103",
    "CEAB3": "45242914000105",
    "CEBR6": "00070698000111",
    "CEDO4": "17245234000100",
    "CEEB3": "15139629000194",
    "CEED3": "08467115000100",
    "CEGR3": "33938119000169",
    "CGAS5": "61856571000117",
    "CGRA4": "92012467000170",
    "CLSC4": "83878892000155",
    "CMIG3": "17155730000164",
    "CMIG4": "17155730000164",
    "CMIN3": "08902291000115",
    "COCE5": "07047251000170",
    "COGN3": "02800026000140",
    "COMR3": "25369840000157",
    "CPFE3": "02429144000193",
    "CPLE3": "76483817000120",
    "CPLE6": "76483817000120",
    "CRPG5": "15115504000124",
    "CRTE3": "00938574000105",
    "CSAN3": "50746577000115",
    "CSED3": "62984091000102",
    "CSMG3": "17281106000103",
    "CSNA3": "33042730000104",
    "CSUD3": "01896779000138",
    "CTAX3": "04032433000180",
    "CTCA3": "06981381000113",
    "CTKA4": "82640558000104",
    "CTSA3": "21255567000189",
    "CURY3": "08797760000183",
    "CVCB3": "10760260000119",
    "CXSE3": "22543331000100",
    "CYRE3": "73178600000118",
    "DASA3": "61486650000183",
    "DESK3": "08170849000115",
    "DEXP3": "02193750000152",
    "DIRR3": "16614075000100",
    "DMVF3": "12108897000150",
    "DOHL4": "84683408000103",
    "DOTZ3": "18174270000184",
    "DTCY3": "03303999000136",
    "DXCO3": "97837181000147",
    "EALT4": "82643537000134",
    "ECOR3": "04149454000180",
    "EGGY3": "81616807000155",
    "EGIE3": "02474103000119",
    "EKTR4": "02328280000197",
    "EMAE4": "02302101000142",
    "EMBJ3": "07689002000189",
    "ENEV3": "04423567000121",
    "ENGI11": "00864214000106",
    "ENJU3": "16922038000151",
    "ENMT3": "03467321000199",
    "EPAR3": "42331462000131",
    "EQMA3B": "06272793000184",
    "EQPA3": "04895728000180",
    "EQTL3": "03220438000173",
    "ESPA3": "26659061000159",
    "ESTR4": "61082004000150",
    "ETER3": "61092037000181",
    "EUCA4": "56643018000166",
    "EUFA3": "61190096000192",
    "EVEN3": "43470988000165",
    "EZTC3": "08312229000173",
    "FESA3": "15141799000103",
    "FESA4": "15141799000103",
    "FHER3": "22266175000188",
    "FICT3": "00359742000108",
    "FIEI3": "07820907000146",
    "FIGE3": "01548981000179",
    "FIQE3": "02255187000108",
    "FLRY3": "60840055000131",
    "FRAS3": "88610126000129",
    "FRIO3": "04821041000108",
    "G2DI33": "38307135000177",
    "GEPA4": "02998301000181",
    "GFSA3": "01545826000107",
    "GGBR3": "33611500000119",
    "GGBR4": "33611500000119",
    "GGPS3": "09229201000130",
    "GMAT3": "24990777000109",
    "GOAU3": "92690783000109",
    "GOAU4": "92690783000109",
    "GOLL54": "06164253000187",
    "GPAR3": "08560444000193",
    "GRND3": "89850341000160",
    "GSHP3": "08764621000153",
    "HAGA4": "30540991000166",
    "HAPV3": "05197443000138",
    "HBOR3": "49263189000102",
    "HBRE3": "14785152000151",
    "HBSA3": "12648327000153",
    "HBTS5": "87762563000103",
    "HETA4": "92749225000163",
    "HMOB3": "40159947000164",
    "HOOT4": "33200049000147",
    "HYPE3": "02932074000191",
    "IFCM3": "38456921000136",
    "IGSN3": "08159965000133",
    "IGTI11": "60543816000193",
    "INEP3": "76627504000106",
    "INNC3": "09611768000176",
    "INTB3": "82901000000127",
    "IRBR3": "33376989000191",
    "ISAE4": "02998611000104",
    "ITSA3": "61532644000115",
    "ITSA4": "61532644000115",
    "ITUB3": "60872504000123",
    "ITUB4": "60872504000123",
    "IVPR3B": "03758318000124",
    "JALL3": "02635522000195",
    "JBSS32": "49115815000105",
    "JFEN3": "33035536000100",
    "JHSF3": "08294224000165",
    "JOPA3": "87456562000122",
    "JSLG3": "52548435000179",
    "KEPL3": "91983056000169",
    "KLAS3": "09146451000106",
    "KLBN11": "89637490000145",
    "LAND3": "40337136000106",
    "LAVV3": "26462693000128",
    "LEVE3": "60476884000187",
    "LIGT3": "03378521000175",
    "LJQQ3": "96418264021802",
    "LLBI3": "16233389000155",
    "LMED3": "02357251000153",
    "LOGG3": "09041168000110",
    "LOGN3": "42278291000124",
    "LPSB3": "08078847000109",
    "LREN3": "92754738000162",
    "LTEL3B": "00743065000127",
    "LTLA3B": "05495546000184",
    "LUPA3": "89463822000112",
    "LUXM4": "92660570000126",
    "LWSA3": "02351877000152",
    "MAPT4": "93828986000173",
    "MATD3": "16676520000159",
    "MBRF3": "03853896000140",
    "MDIA3": "07206816000115",
    "MDNE3": "12049631000184",
    "MEAL3": "17314329000120",
    "MELK3": "12181987000177",
    "MERC4": "33040601000187",
    "MGEL4": "61065298000102",
    "MGLU3": "47960950000121",
    "MILS3": "27093558000115",
    "MLAS3": "59717553000102",
    "MMAQ4": "17161241000115",
    "MNDL3": "88610191000154",
    "MNPR3": "90076886000140",
    "MOTV3": "02846056000197",
    "MOVI3": "21314559000166",
    "MRSA3B": "01417222000177",
    "MRVE3": "08343492000120",
    "MSPA3": "60730348000166",
    "MTRE3": "07882930000165",
    "MTSA4": "86375425000109",
    "MULT3": "07816890000153",
    "MWET4": "84683671000194",
    "MYPK3": "61156113000175",
    "NATU3": "71673990000177",
    "NEMO5": "60651809000105",
    "NEOE3": "01083200000118",
    "NEXP3": "08613550000198",
    "NGRD3": "10139870000108",
    "NORD3": "60884319000159",
    "NUTR3": "51128999000190",
    "OBTC3": "59693110000129",
    "ODER4": "97191902000194",
    "ODPV3": "58119199000151",
    "OFSA3": "20258278000170",
    "OIBR3": "76535764000143",
    "ONCO3": "12104241000402",
    "OPCT3": "09114805000130",
    "OPGM3": "02796775000140",
    "OPSE3": "02062747000108",
    "OPTS3": "01957772000189",
    "ORVR3": "11421994000136",
    "OSXB3": "09112685000132",
    "PASS3": "21389501000181",
    "PATI3": "92693019000189",
    "PCAR3": "47508411000156",
    "PDGR3": "02950811000189",
    "PDTC3": "02365069000144",
    "PEAB4": "01938783000111",
    "PETR3": "33000167000101",
    "PETR4": "33000167000101",
    "PFRM3": "45453214000151",
    "PGMN3": "06626253000151",
    "PINE4": "62144175000120",
    "PLAS3": "51928174000150",
    "PLPL3": "24230275000180",
    "PMAM3": "60398369000126",
    "PNVL3": "92665611000177",
    "PNVL4": "92665611000177",
    "POMO3": "88611835000129",
    "POMO4": "88611835000129",
    "POSI3": "81243735000148",
    "PPAR3": "59789545000171",
    "PPLA11": "15073274000188",
    "PRIO3": "10629105000168",
    "PRNR3": "18593815000197",
    "PRPT3": "02992449000109",
    "PSSA3": "02149205000169",
    "PTBL3": "83475913000191",
    "PTCA3": "08574411000100",
    "PTNT4": "88613658000110",
    "QUAL3": "11992680000193",
    "QUSW3": "35791391000194",
    "QVQP3": "01851771000155",
    "RADL3": "61585865000151",
    "RAIL3": "02387241000160",
    "RAIZ4": "33453598000123",
    "RANI3": "92791243000103",
    "RAPT4": "89086144000116",
    "RBNS11": "59981829000165",
    "RCSL4": "91333666000117",
    "RDNI3": "67010660000124",
    "RDOR3": "06047087000139",
    "RECV3": "03342704000130",
    "REDE3": "61584140000149",
    "RENT3": "16670085000155",
    "RIAA3": "08402943000152",
    "RIOS3": "38199406000118",
    "RNEW4": "08534605000174",
    "ROMI3": "56720428000163",
    "RPAD5": "17167396000169",
    "RPMG3": "33412081000196",
    "RSID3": "61065751000180",
    "RSUL4": "85778074000106",
    "RVEE3": "52841191000118",
    "SALT3": "17765891000170",
    "SANB11": "90400888000142",
    "SAPR11": "76484013000145",
    "SBFG3": "13217485000111",
    "SBSP3": "43776517000180",
    "SCAR3": "29780061000109",
    "SEER3": "04986320000113",
    "SEQL3": "01599101000193",
    "SHOW3": "02860694000162",
    "SHUL3": "84693183000168",
    "SHUL4": "84693183000168",
    "SIMH3": "07415333000120",
    "SLCE3": "89096457000155",
    "SMFT3": "07594978000178",
    "SMTO3": "51466860000156",
    "SNSY5": "14807945000124",
    "SOJA3": "10807374000177",
    "SOND5": "33386210000119",
    "SRNA3": "42500384000151",
    "SUZB3": "16404287000155",
    "SYNE3": "08801621000186",
    "TAEE11": "07859971000130",
    "TASA4": "92781335000102",
    "TCSA3": "08065557000112",
    "TECN3": "09295063000197",
    "TEKA4": "82636986000155",
    "TELB4": "00336701000104",
    "TEND3": "71476527000135",
    "TFCO4": "59418806000147",
    "TGMA3": "02351144000118",
    "TIMS3": "02421421000111",
    "TKNO4": "33467572000134",
    "TOKY3": "31553627000101",
    "TOTS3": "53113791000122",
    "TPIS3": "03014553000191",
    "TRAD3": "26345998000150",
    "TRIS3": "08811643000127",
    "TTEN3": "94813102000170",
    "TUPY3": "84683374000149",
    "TXRX4": "82982075000180",
    "UCAS3": "90441460000148",
    "UGPA3": "33256439000139",
    "UNIP6": "33958695000178",
    "USIM3": "60894730000105",
    "USIM5": "60894730000105",
    "VALE3": "33592510000154",
    "VAMO3": "23373000000132",
    "VBBR3": "34274233000102",
    "VITT3": "45365558000109",
    "VIVA3": "33839910000111",
    "VIVR3": "67571414000141",
    "VIVT3": "02558157000162",
    "VLID3": "33113309000147",
    "VSTE3": "49669856000143",
    "VTRU3": "20512706000140",
    "VULC3": "50926955000142",
    "VVEO3": "12420164000157",
    "WDCN3": "05917486000140",
    "WEGE3": "84429695000111",
    "WEST3": "14776142000150",
    "WHRL4": "59105999000186",
    "WIZC3": "42278473000103",
    "WLMM4": "33228024000151",
    "YDUQ3": "08807432000110",
    "ZAMP3": "13574594000196",
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
        self._cnpj_to_setor: dict[str, str] = {}
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
            setor = str(row.get("SETOR_ATIV", "")).strip()

            if cnpj and cd_cvm:
                self._cnpj_to_cvm[cnpj] = cd_cvm
                self._cvm_to_cnpj[cd_cvm] = cnpj
                self._cvm_to_name[cd_cvm] = nome
                self._cnpj_to_setor[cnpj] = setor if setor and setor != "nan" else ""

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

    def get_setor(self, cnpj: str) -> str:
        """Retorna setor de atividade pelo CNPJ."""
        cnpj_clean = cnpj.replace(".", "").replace("/", "").replace("-", "")
        return self._cnpj_to_setor.get(cnpj_clean, "")

    def get_all_mapped_companies(self) -> list[dict]:
        """
        Retorna todas as empresas que têm ticker mapeado.
        Usado pelo screener.
        """
        seen_cnpjs = set()
        companies = []
        for ticker, cnpj in TICKER_TO_CNPJ.items():
            if cnpj in seen_cnpjs:
                continue
            seen_cnpjs.add(cnpj)
            cd_cvm = self._cnpj_to_cvm.get(cnpj)
            if not cd_cvm:
                continue
            tickers = CNPJ_TO_TICKERS.get(cnpj, [ticker])
            companies.append({
                "ticker": tickers[0],  # Primary ticker
                "tickers": tickers,
                "cnpj": cnpj,
                "cd_cvm": cd_cvm,
                "nome": self._cvm_to_name.get(cd_cvm, ""),
                "setor": self._cnpj_to_setor.get(cnpj, ""),
            })
        return companies


# Singleton
ticker_mapper = TickerMapper()
