import argparse
from strategy import run_strategy

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run trading strategy")

    parser.add_argument(
        "--broker",
        type=str,
        required=True,
        help="Broker name (e.g., acg, icmarkets, xm)"
    )

    parser.add_argument(
        "--vol_target",
        type=float,
        default=0.10,
        help="Volatility target"
    )

    parser.add_argument(
        "--cov_method",
        type=str,
        default="ewma",
        help="Volatility method (ewma or garch)"
    )

    parser.add_argument(
        "--ewma_lambda",
        type=float,
        default=0.94,
        help="EWMA decay factor"
    )

    parser.add_argument(
        "--ic",
        type=float,
        default=0.15,
        help="Information coefficient"
    )

    args = parser.parse_args()

    broker_name = args.broker

df, gross_target, gross_current = run_strategy(
    broker_name=broker_name,
    macro_signal_csv=f"{broker_name}_macro_signal.csv",
    vol_target=args.vol_target,
    cov_method=args.cov_method,
    ewma_lambda=args.ewma_lambda,
    ic=args.ic
)

print(df[[
    "asset",
    "current_weight",
    "current_holdings",
    "scaled_weight",
    "target_lot_size",
    "adjusted_lot_size",       # ← NEW
    "adjusted_difference"      # ← NEW (final trade instruction)
]])

print(f"gross current holdings: {gross_current:0.2f}")
print(f"gross target lots: {gross_target:0.2f}")

from execution import execute_rebalance

# after you get df from run_strategy / run_lot_sizing
#results = execute_rebalance(df, broker_name)

#for r in results:
#    print(r)