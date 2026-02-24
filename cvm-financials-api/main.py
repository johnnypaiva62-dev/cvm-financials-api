"""
Ponto de entrada para rodar a API.

Uso:
    python main.py                    # Roda a API (porta 8000)
    python main.py --port 3000        # Porta customizada
    python main.py --no-cache         # Força re-download da CVM
"""

import argparse
import uvicorn


def main():
    parser = argparse.ArgumentParser(description="CVM Financials API")
    parser.add_argument(
        "--port", type=int, default=8000, help="Porta (default: 8000)"
    )
    parser.add_argument(
        "--host", type=str, default="0.0.0.0", help="Host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--reload", action="store_true", help="Hot reload (dev mode)"
    )
    args = parser.parse_args()

    print(f"\n🚀 Iniciando CVM Financials API em {args.host}:{args.port}")
    print(f"📄 Docs: http://localhost:{args.port}/docs\n")

    uvicorn.run(
        "app.api:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
