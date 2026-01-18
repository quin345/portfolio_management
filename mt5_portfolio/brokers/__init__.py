from .icmarkets import ICMarkets

def get_broker(name: str):
    name = name.lower()

    if name in ("icm", "icmarkets", "ic markets"):
        return ICMarkets()
    raise ValueError(f"Unknown broker: {name}")