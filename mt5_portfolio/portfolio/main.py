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
        "--method",
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
        default=0.05,
        help="Information coefficient"
    )

    args = parser.parse_args()

    broker_name = args.broker

    df, gross_target, gross_current = run_strategy(
        broker_name=broker_name,
        macro_signal_csv=f"{broker_name}_macro_signal.csv",
        vol_target=args.vol_target,
        method=args.method,
        ewma_lambda=args.ewma_lambda,
        ic=args.ic
    )

    print(df[["asset", "contract_size", "min_volume", "current_holdings", "target_lot_size"]])