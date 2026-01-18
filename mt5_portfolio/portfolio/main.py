from strategy import run_strategy

if __name__ == "__main__":

    df, gross_target, gross_current = run_strategy(
        broker_name="aquafunded",
        active_symbols_csv= "active_symbols.csv",
        factor_signal_csv="ai_macro_signal.csv",
        vol_target=0.10,
        method="ewma",
        ewma_lambda=0.94,
        ic=0.05
    )

print(df[["asset", "contract_size", "min_volume", "current_holdings", "target_lot_size"]])


