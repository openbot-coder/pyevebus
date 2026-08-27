"""
evebus 入口

支持:
  python -m evebus serve/run ...  → 服务端
  python -m evebus <command>      → 客户端 (evebusctl)
"""
import sys

if len(sys.argv) > 1 and sys.argv[1] in ("serve", "run"):
    from .server_cli import server_cli as cli
else:
    from .cli import cli

if __name__ == "__main__":
    cli()