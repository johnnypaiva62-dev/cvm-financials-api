# CVM Financials API 🇧🇷

API REST para consulta de demonstrativos financeiros de empresas listadas na B3,
com dados direto do portal de **Dados Abertos da CVM**.

**Demonstrativos disponíveis:**
- **DRE** — Demonstração do Resultado do Exercício
- **Balanço Patrimonial** — Ativo (BPA) e Passivo/PL (BPP)
- **DFC** — Demonstração do Fluxo de Caixa

100% cloud, sem dependência de Excel ou Economatica.

---

## Início rápido (local)

```bash
# 1. Clone o repositório
git clone https://github.com/seu-usuario/cvm-financials-api.git
cd cvm-financials-api

# 2. Instale dependências
pip install -r requirements.txt

# 3. Rode a API
python main.py
```

A API inicia em `http://localhost:8000`.
Na primeira execução, **baixa os dados da CVM** (~200-400MB de ZIPs) e processa.
Isso leva **2-5 minutos** dependendo da conexão.
Após o primeiro download, os dados ficam em cache local (`data/cache/`).

Acesse a **documentação interativa**: `http://localhost:8000/docs`

---

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/` | Health check |
| GET | `/status` | Status e contagem de registros |
| GET | `/empresas?search=petrobras` | Lista de empresas (busca por nome) |
| GET | `/empresa/{cd_cvm}` | Todos os demonstrativos de uma empresa |
| GET | `/dre?cd_cvm=9512` | DRE (filtro por empresa/data) |
| GET | `/balanco/ativo?cd_cvm=9512` | Balanço Patrimonial — Ativo |
| GET | `/balanco/passivo?cd_cvm=9512` | Balanço Patrimonial — Passivo/PL |
| GET | `/dfc?cd_cvm=9512` | Fluxo de Caixa |
| GET | `/contas/DRE` | Contas rastreadas na DRE |
| POST | `/reload?use_cache=false` | Força re-download dos dados |

### Parâmetros de filtro comuns

| Parâmetro | Descrição | Exemplo |
|-----------|-----------|---------|
| `cd_cvm` | Código CVM da empresa | `9512` (Petrobras) |
| `cnpj` | CNPJ (com ou sem pontuação) | `33000167000101` |
| `dt_refer` | Data de referência | `2024-09-30` |
| `raw` | Dados não-pivotados | `true` |
| `limit` | Registros por página | `100` |
| `offset` | Paginação | `0` |

### Exemplo de chamada

```bash
# DRE da Petrobras, últimos trimestres
curl "http://localhost:8000/dre?cd_cvm=9512"

# Buscar empresa por nome
curl "http://localhost:8000/empresas?search=vale"

# Todos os dados financeiros de uma empresa
curl "http://localhost:8000/empresa/9512"
```

### Exemplo de resposta (`/dre?cd_cvm=9512`)

```json
{
  "data": [
    {
      "CNPJ_CIA": "33.000.167/0001-01",
      "DENOM_CIA": "PETRÓLEO BRASILEIRO S.A. - PETROBRAS",
      "CD_CVM": "9512",
      "DT_REFER": "2024-09-30",
      "Receita Líquida": 129788000000.0,
      "Lucro Bruto": 62145000000.0,
      "EBIT": 45231000000.0,
      "Lucro/Prejuízo do Período": 32579000000.0
    }
  ],
  "total": 12,
  "limit": 500,
  "offset": 0
}
```

---

## Deploy no cloud

### Railway (recomendado)

1. Crie conta em [railway.app](https://railway.app)
2. Conecte o repositório GitHub
3. Railway detecta o `Dockerfile` automaticamente
4. Configure a variável de ambiente `PORT=8000` (se necessário)
5. Deploy automático! URL tipo: `https://cvm-api-production.up.railway.app`

### Render

1. Crie conta em [render.com](https://render.com)
2. New → Web Service → conecte o repo
3. Environment: Docker
4. Deploy!

### Fly.io

```bash
fly launch
fly deploy
```

---

## Integração com Lovable

No seu app Lovable, faça chamadas à API:

```javascript
const API_URL = "https://sua-api.railway.app";

// Buscar empresas
const res = await fetch(`${API_URL}/empresas?search=petrobras`);
const { data } = await res.json();

// DRE de uma empresa
const dre = await fetch(`${API_URL}/dre?cd_cvm=${data[0].CD_CVM}`);
const { data: dreData } = await dre.json();

// Todos os demonstrativos
const all = await fetch(`${API_URL}/empresa/${data[0].CD_CVM}`);
const financials = await all.json();
// financials.DRE, financials.BPA, financials.BPP, financials.DFC
```

---

## Estrutura do projeto

```
cvm-financials-api/
├── app/
│   ├── __init__.py
│   ├── api.py           # FastAPI — endpoints
│   ├── downloader.py    # Download dos ZIPs da CVM
│   ├── parser.py        # Limpeza e estruturação dos dados
│   └── service.py       # Orquestra download + parse + consultas
├── data/
│   └── cache/           # Cache dos ZIPs baixados (git-ignored)
├── main.py              # Entry point
├── requirements.txt
├── Dockerfile
├── .gitignore
└── README.md
```

## Contas rastreadas

### DRE
| Código | Descrição |
|--------|-----------|
| 3.01 | Receita Líquida |
| 3.03 | Resultado Bruto |
| 3.05 | EBIT |
| 3.06 | Resultado Financeiro |
| 3.11 | Lucro/Prejuízo do Período |

### Balanço (BPA + BPP)
| Código | Descrição |
|--------|-----------|
| 1 | Ativo Total |
| 1.01.01 | Caixa e Equivalentes |
| 2 | Passivo Total |
| 2.03 | Patrimônio Líquido Consolidado |

### DFC
| Código | Descrição |
|--------|-----------|
| 6.01 | Caixa Líquido Ativ. Operacionais |
| 6.02 | Caixa Líquido Ativ. Investimento |
| 6.03 | Caixa Líquido Ativ. Financiamento |

> Veja a lista completa em `app/parser.py` — personalize as contas conforme sua necessidade.

---

## Notas

- **Dados fonte**: [dados.cvm.gov.br](https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/)
- **Atualização**: Os dados da CVM são atualizados diariamente. Use `POST /reload` para atualizar.
- **Cache**: ZIPs ficam em `data/cache/`. Delete para forçar re-download.
- **Encoding**: O parser já lida com BOM e encoding latin-1 dos CSVs da CVM.
- **Consolidado vs Individual**: Por padrão usa demonstrativos **consolidados**.
