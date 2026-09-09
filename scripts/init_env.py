"""Generate local credentials without printing them or replacing an existing .env."""

import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
target = root / ".env"
if target.exists():
    print("Existing .env preserved.")
else:
    template = (root / ".env.example").read_text(encoding="utf-8")
    for name in ("POSTGRES_PASSWORD", "MINIO_ROOT_PASSWORD"):
        template = template.replace(f"{name}=\n", f"{name}={secrets.token_hex(24)}\n")
    with target.open("x", encoding="utf-8") as output:
        output.write(template)
    print("Created .env with random local credentials. Do not commit this file.")
