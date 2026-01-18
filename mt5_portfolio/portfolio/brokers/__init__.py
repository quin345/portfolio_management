from .icmarkets import ICMarkets
from .aquafunded import AquaFunded
from .acg import ACG

def get_broker(name: str):
    name = name.lower()

    if name in ("icm", "icmarkets", "ic markets"):
        return ICMarkets()
    if name in ("aquafunded", "aqua"):
        return AquaFunded()
    if name in ("acg"):
        return ACG()
    raise ValueError(f"Unknown broker: {name}")