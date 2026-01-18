from .icmarkets import ICMarkets
from .aquafunded import AquaFunded

def get_broker(name: str):
    name = name.lower()

    if name in ("icm", "icmarkets", "ic markets"):
        return ICMarkets()
    if name in ("aquafunded", "aqua"):
        return AquaFunded()
    raise ValueError(f"Unknown broker: {name}")