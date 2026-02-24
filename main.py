"""
Ponto de entrada para rodar a API.

Uso local:
    python main.py
"""

import os
import uvicorn


def main():
    # Railway define PORT via variável de ambiente
    port = int(os.environ.get("PORT", 8000))
    host = "0.0.0.0"

    print(f"\n🚀 CVM Financials API em {host}:{port}")
    print(f"📄 Docs: http://localhost:{port}/docs\n")

    uvicorn.run("app.api:app", host=host, port=port)


if __name__ == "__main__":
    main()
