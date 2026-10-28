import requests
from src.config import OPENROUTER_API_KEY

r = requests.get(
    "https://openrouter.ai/api/v1/models",
    headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
)
models = r.json()["data"]

print("=== FREE models with tool calling ===\n")
for m in models:
    supports_tools = "tools" in (m.get("supported_parameters") or [])
    prompt_price = float(m.get("pricing", {}).get("prompt", "1"))
    if supports_tools and prompt_price == 0:
        print(f"  {m['id']}")

print("\n=== CHEAP paid models with tool calling (top 15 by price) ===\n")
paid = [
    m for m in models
    if "tools" in (m.get("supported_parameters") or [])
    and float(m.get("pricing", {}).get("prompt", "1")) > 0
]
paid.sort(key=lambda m: float(m["pricing"]["prompt"]))
for m in paid[:15]:
    price = float(m["pricing"]["prompt"]) * 1_000_000
    print(f"  {m['id']}  (${price:.3f}/M tokens)")