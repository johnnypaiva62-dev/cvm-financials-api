"""
B3 BTC (Empréstimo de Ativos) - Data fetcher
==============================================
Fetches open lending positions from B3's daily bulletin (BDI_05 PDF).

Data source: B3 Boletim Diário - Clearing - Posição em Aberto
URL: https://arquivos.b3.com.br/bdi/download/bdi/{date}/BDI_05_{date_compact}.pdf

Requires: pip install pdfplumber
"""

import logging
import re
import io
from datetime import datetime, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    logger.warning("pdfplumber não instalado. pip install pdfplumber")

_btc_cache = {}
_btc_cache_date = None


def _get_last_business_days(n=10):
    days = []
    d = datetime.now().date()
    while len(days) < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d)
    return days


def _try_fetch_btc_pdf(date_obj) -> Optional[bytes]:
    date_str = date_obj.strftime("%Y-%m-%d")
    date_compact = date_obj.strftime("%Y%m%d")
    url = f"https://arquivos.b3.com.br/bdi/download/bdi/{date_str}/BDI_05_{date_compact}.pdf"

    # Use a full browser-like session to avoid B3 blocking cloud IPs
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.b3.com.br/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-site",
    })

    try:
        # First hit the main page to get cookies
        try:
            session.get("https://arquivos.b3.com.br/bdi/", timeout=10)
        except Exception:
            pass

        logger.info(f"BTC: Tentando {url}")
        resp = session.get(url, timeout=30)
        if resp.status_code == 200 and len(resp.content) > 1000 and resp.content[:5] == b"%PDF-":
            logger.info(f"BTC: PDF obtido ({len(resp.content)} bytes)")
            return resp.content
        else:
            logger.warning(f"BTC: status={resp.status_code} len={len(resp.content)}")
    except Exception as e:
        logger.debug(f"BTC: Falha {url}: {e}")
    return None


def _parse_number_br(val: str) -> Optional[float]:
    """Parse '1.234.567,89' → 1234567.89"""
    if not val or val.strip() in ("-", ""):
        return None
    try:
        return float(val.strip().replace(".", "").replace(",", "."))
    except (ValueError, TypeError):
        return None


def _parse_btc_pdf(pdf_bytes: bytes) -> list[dict]:
    """Parse BDI_05 PDF using pdfplumber table extraction."""
    if not HAS_PDFPLUMBER:
        return []

    records = []
    in_section = False

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                # Try table extraction first (more reliable)
                tables = page.extract_tables()
                
                for table in tables:
                    for row in table:
                        if not row or len(row) < 3:
                            continue
                        
                        row_text = " ".join(str(c or "") for c in row)
                        
                        # Detect section
                        if "Posição em Aberto" in row_text and "Empréstimo" in row_text:
                            in_section = True
                            continue
                        if in_section and "Empréstimos Registrados" in row_text:
                            in_section = False
                            continue
                        
                        if not in_section:
                            continue
                        
                        # Skip headers
                        if any(h in row_text for h in ["Ticker", "ISIN", "Saldo em quantidade",
                                                        "Preço Médio", "Empresa ou Fundo"]):
                            continue
                        
                        rec = _parse_table_row(row)
                        if rec:
                            records.append(rec)
                
                # Fallback: text-based parsing if no tables found
                if not records and not tables:
                    text = page.extract_text() or ""
                    for line in text.split("\n"):
                        line = line.strip()
                        if "Posição em Aberto" in line and "Empréstimo" in line:
                            in_section = True
                            continue
                        if in_section and "Empréstimos Registrados" in line:
                            in_section = False
                            continue
                        if not in_section:
                            continue
                        rec = _parse_text_line(line)
                        if rec:
                            records.append(rec)

    except Exception as e:
        logger.error(f"BTC: Erro ao parsear PDF: {e}", exc_info=True)

    logger.info(f"BTC: {len(records)} registros parseados do PDF")
    return records


def _parse_table_row(row: list) -> Optional[dict]:
    """Parse a row extracted from a PDF table."""
    # Clean cells
    cells = [str(c or "").strip() for c in row]
    
    # Find ticker - looks for pattern like PETR4, VALE3, BBDC4
    ticker = None
    ticker_idx = None
    for i, c in enumerate(cells):
        if re.match(r"^[A-Z]{4}\d{1,2}$", c):
            ticker = c
            ticker_idx = i
            break
        # Also match tickers with 3 letters (e.g. B3SA3)  
        if re.match(r"^[A-Z0-9]{4,6}\d{1,2}$", c) and any(ch.isalpha() for ch in c):
            ticker = c
            ticker_idx = i
            break
    
    if not ticker:
        return None
    
    # Try to find tipo (Registro, Voluntário, Total)
    tipo = ""
    for c in cells:
        if c in ("Registro", "Voluntário", "Automático", "Total"):
            tipo = c
            break
    
    # Find numeric values - walk backwards from end
    nums = []
    for c in reversed(cells):
        val = _parse_number_br(c)
        if val is not None:
            nums.insert(0, val)
        elif c == "-":
            nums.insert(0, None)
        elif nums:
            break  # Stop when we hit text after finding numbers
    
    if len(nums) < 2:
        return None
    
    # nums should be: [qty, price, value] or [qty, value]
    qty = nums[0]
    price = nums[1] if len(nums) >= 3 else None
    value = nums[-1]
    
    if not qty or qty == 0:
        return None
    
    # Find empresa name
    empresa = ""
    for c in cells:
        if c.startswith("BR") and len(c) == 12:
            continue
        if c == ticker:
            continue
        if c in ("Registro", "Voluntário", "Automático", "Total", "Balcão", "Eletrônico"):
            continue
        if re.match(r"^\d{2}/\d{2}/\d{4}$", c):
            continue
        if _parse_number_br(c) is not None or c == "-":
            continue
        if len(c) > 3 and any(ch.isalpha() for ch in c):
            empresa = c
            break
    
    return {
        "ticker": ticker,
        "empresa": empresa,
        "tipo": tipo,
        "saldo_qty": int(qty),
        "preco_medio": round(price, 4) if price else None,
        "saldo_brl": round(value, 2) if value else None,
    }


def _parse_text_line(line: str) -> Optional[dict]:
    """Fallback: parse a text line from PDF."""
    # Remove date prefix
    line = re.sub(r"^\d{2}/\d{2}/\d{4}\s+", "", line)
    parts = line.split()
    if len(parts) < 4:
        return None
    
    ticker = parts[0]
    if not re.match(r"^[A-Z0-9]{4,6}\d{0,2}$", ticker):
        return None
    
    # Find numbers at end
    nums = []
    for p in reversed(parts):
        val = _parse_number_br(p)
        if val is not None:
            nums.insert(0, val)
        elif p == "-":
            nums.insert(0, None)
        elif nums:
            break
    
    if len(nums) < 2:
        return None
    
    qty = nums[0]
    price = nums[1] if len(nums) >= 3 else None
    value = nums[-1]
    
    if not qty or qty == 0:
        return None
    
    # Detect tipo
    tipo = ""
    for t in ("Total", "Registro", "Voluntário"):
        if t in parts:
            tipo = t
            break
    
    return {
        "ticker": ticker,
        "empresa": "",
        "tipo": tipo,
        "saldo_qty": int(qty),
        "preco_medio": round(price, 4) if price else None,
        "saldo_brl": round(value, 2) if value else None,
    }


def fetch_btc_data() -> dict:
    """Fetch and cache BTC data from B3."""
    global _btc_cache, _btc_cache_date

    if not HAS_PDFPLUMBER:
        return {"date": None, "count": 0, "positions": [],
                "error": "pdfplumber não instalado. Execute: pip install pdfplumber"}

    today = datetime.now().date()
    if _btc_cache_date == today and _btc_cache:
        logger.info("BTC: Usando cache")
        return _btc_cache

    for d in _get_last_business_days(10):
        pdf_bytes = _try_fetch_btc_pdf(d)
        if pdf_bytes:
            records = _parse_btc_pdf(pdf_bytes)
            if records:
                # Prefer "Total" rows (already aggregated per ticker)
                totals = [r for r in records if r.get("tipo") == "Total"]
                use = totals if totals else records

                result = {
                    "date": d.isoformat(),
                    "count": len(use),
                    "positions": use,
                }
                _btc_cache = result
                _btc_cache_date = today
                logger.info(f"BTC: {len(use)} posições de {d.isoformat()}")
                return result

    return {"date": None, "count": 0, "positions": []}


def get_btc_for_ticker(ticker: str, btc_data: dict = None) -> dict:
    """Get BTC lending data for a specific ticker."""
    if btc_data is None:
        btc_data = fetch_btc_data()

    if btc_data.get("error"):
        return {
            "date": None, "ticker": ticker, "found": False,
            "error": btc_data["error"],
            "saldo_qty": 0, "saldo_brl": 0, "preco_medio": None,
        }

    positions = btc_data.get("positions", [])
    base = ticker.upper()
    matches = [p for p in positions if p["ticker"].upper() == base]

    if not matches:
        return {
            "date": btc_data.get("date"), "ticker": ticker, "found": False,
            "saldo_qty": 0, "saldo_brl": 0, "preco_medio": None,
        }

    total_qty = sum(p["saldo_qty"] for p in matches)
    total_brl = sum(p["saldo_brl"] or 0 for p in matches)
    avg_price = total_brl / total_qty if total_qty > 0 else None

    return {
        "date": btc_data.get("date"),
        "ticker": ticker,
        "found": True,
        "saldo_qty": total_qty,
        "saldo_brl": round(total_brl, 2),
        "preco_medio": round(avg_price, 4) if avg_price else None,
        "details": matches,
    }


def debug_btc_fetch() -> dict:
    """Debug function: try fetching BTC PDF and report what happens."""
    info = {
        "pdfplumber_installed": HAS_PDFPLUMBER,
        "attempts": [],
    }

    for d in _get_last_business_days(3):
        date_str = d.strftime("%Y-%m-%d")
        date_compact = d.strftime("%Y%m%d")
        url = f"https://arquivos.b3.com.br/bdi/download/bdi/{date_str}/BDI_05_{date_compact}.pdf"

        attempt = {"date": date_str, "url": url}
        try:
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "application/pdf,text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
                "Referer": "https://www.b3.com.br/",
            })
            # Pre-visit to get cookies
            try:
                cr = session.get("https://arquivos.b3.com.br/bdi/", timeout=10)
                attempt["cookie_status"] = cr.status_code
                attempt["cookies"] = list(session.cookies.keys())
            except Exception as ce:
                attempt["cookie_error"] = str(ce)

            resp = session.get(url, timeout=30)
            attempt["status_code"] = resp.status_code
            attempt["content_length"] = len(resp.content)
            attempt["content_type"] = resp.headers.get("Content-Type", "?")
            attempt["is_pdf"] = resp.content[:5] == b"%PDF-" if resp.content else False
            attempt["first_bytes"] = resp.content[:100].decode("latin-1", errors="replace") if resp.content else ""

            if resp.status_code == 200 and attempt["is_pdf"] and HAS_PDFPLUMBER:
                # Try parsing
                import pdfplumber as plumb
                with plumb.open(io.BytesIO(resp.content)) as pdf:
                    attempt["num_pages"] = len(pdf.pages)
                    # Check first page text
                    text = pdf.pages[0].extract_text() or ""
                    attempt["first_page_chars"] = len(text)
                    attempt["has_emprestimo"] = "Empréstimo" in text or "Posição em Aberto" in text
                    
                    # Search all pages for lending section
                    for i, page in enumerate(pdf.pages):
                        pt = page.extract_text() or ""
                        if "Empréstimo" in pt and "Posição em Aberto" in pt:
                            attempt["lending_section_page"] = i + 1
                            # Try to get a sample
                            lines = pt.split("\n")
                            sample = [l for l in lines if "PETR" in l or "VALE" in l][:3]
                            attempt["sample_lines"] = sample
                            break
                    
                    # Also try full parse
                    records = _parse_btc_pdf(resp.content)
                    attempt["parsed_records"] = len(records)
                    if records:
                        attempt["sample_records"] = records[:3]

        except Exception as e:
            attempt["error"] = str(e)

        info["attempts"].append(attempt)

    return info
