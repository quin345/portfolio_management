import talib
import MetaTrader5 as mt5
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report


def generate_features(df):
    """
    df must contain: ['open','high','low','close','volume']
    returns a DataFrame of continuous ML features
    """

    o = df['open']
    h = df['high']
    l = df['low']
    c = df['close']
    v = df['volume']

    feats = pd.DataFrame(index=df.index)

    # ============================
    # MEAN REVERSION INDICATORS
    # ============================
    feats['rsi_14'] = talib.RSI(c, timeperiod=14)
    feats['stoch_k'], feats['stoch_d'] = talib.STOCH(h, l, c)
    feats['cci_20'] = talib.CCI(h, l, c, timeperiod=20)
    feats['willr_14'] = talib.WILLR(h, l, c, timeperiod=14)
    feats['mfi_14'] = talib.MFI(h, l, c, v, timeperiod=14)

    # Bollinger Bands
    upper, middle, lower = talib.BBANDS(c, timeperiod=20, nbdevup=2, nbdevdn=2)
    feats['bb_width'] = (upper - lower) / middle
    feats['bb_percent'] = (c - lower) / (upper - lower)

    # ============================
    # TREND / CONTINUATION
    # ============================
    feats['ema_20'] = talib.EMA(c, timeperiod=20)
    feats['ema_50'] = talib.EMA(c, timeperiod=50)
    feats['ema_20_50_diff'] = feats['ema_20'] - feats['ema_50']

    macd, macd_signal, macd_hist = talib.MACD(c)
    feats['macd'] = macd
    feats['macd_signal'] = macd_signal
    feats['macd_hist'] = macd_hist

    feats['roc_10'] = talib.ROC(c, timeperiod=10)
    feats['mom_10'] = talib.MOM(c, timeperiod=10)

    # ADX / Aroon
    feats['adx_14'] = talib.ADX(h, l, c, timeperiod=14)
    aroon_down, aroon_up = talib.AROON(h, l, timeperiod=14)
    feats['aroon_up'] = aroon_up
    feats['aroon_down'] = aroon_down

    # ============================
    # VOLATILITY
    # ============================
    feats['atr_14'] = talib.ATR(h, l, c, timeperiod=14)
    feats['natr_14'] = talib.NATR(h, l, c, timeperiod=14)
    feats['std_20'] = talib.STDDEV(c, timeperiod=20)

    # ============================
    # VOLUME / FLOW
    # ============================
    feats['obv'] = talib.OBV(c, v)
    feats['adosc'] = talib.ADOSC(h, l, c, v)

    # ============================
    # RAW PRICE FEATURES
    # ============================
    feats['ret_1'] = c.pct_change()
    feats['ret_5'] = c.pct_change(5)
    feats['ret_10'] = c.pct_change(10)

    # ============================
    # CLEANUP
    # ============================
    feats = (
        feats.replace([np.inf, -np.inf], np.nan)
            .bfill()
            .ffill()
    )

    return feats



def generate_labels(df, threshold=0.001, compress=False):
    """
    df: OHLCV dataframe containing at least 'close'
    threshold: minimum absolute return required to label 1 or -1
    compress: if True, collapse consecutive identical labels into transitions only

    returns:
        labels: pd.Series of labels {-1, 0, 1}
        counts: dict with counts of each label
    """

    # compute future return
    future_ret = df['close'].pct_change(periods=-1)

    # apply threshold
    labels = np.where(
        future_ret > threshold, 1,
        np.where(
            future_ret < -threshold, -1,
            0
        )
    )

    # convert to Series
    labels = pd.Series(labels, index=df.index, name="label")

    # remove labels where future return is NaN
    labels = labels[future_ret.notna()]

    # ---------------------------------------------------------
    # OPTIONAL: compress consecutive duplicates (state changes)
    # ---------------------------------------------------------
    if compress:
        change = labels != labels.shift(1)
        labels = labels[change]

    # ---------------------------------------------------------
    # Count occurrences of each class
    # ---------------------------------------------------------
    counts = labels.value_counts().to_dict()

    return labels, counts



def evaluate_symbol(df, stddev=2.5):

    threshold = df['close'].pct_change().std() * stddev
    print(f"threshold: {threshold:0.2%}")

    # FEATURES + LABELS
    features = generate_features(df)
    return_labels, counts = generate_labels(df=df, threshold=threshold, compress=False)  
    # IMPORTANT: compress=True but generate_labels must return 0 for non-events

    # ALIGN
    common_index = features.index.intersection(return_labels.index)
    features = features.loc[common_index]
    return_labels = return_labels.loc[common_index]

    # LABEL STATS
    total = len(return_labels)
    counts = return_labels.value_counts().to_dict()
    label_stats = {"total": total, "counts": counts}

    # CLEAN
    mask = ~np.isnan(return_labels)
    X = features[mask]
    y = return_labels[mask]   # contains -1, 0, +1

    # ---------------------------------------------------------
    # MAP TRADING LABELS → MODEL LABELS
    # -1 → 0
    #  0 → 1
    # +1 → 2
    # ---------------------------------------------------------
    y_model = y.replace({-1: 0, 0: 1, 1: 2}).astype(int)

    # SPLIT
    X_train, X_val, y_train, y_val = train_test_split(
        X, y_model, test_size=0.2, shuffle=False
    )

    # ---------------------------------------------------------
    # XGBOOST MULTI-CLASS MODEL
    # ---------------------------------------------------------
    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=3,
        learning_rate=0.05,
        subsample=0.7,
        colsample_bytree=0.7,
        reg_alpha=1.0,
        reg_lambda=1.0,
        tree_method='hist',
        objective='multi:softprob',   # <--- MULTI-CLASS
        num_class=3                   # <--- 3 CLASSES
    )

    # CLASS WEIGHTS (optional)
    total = sum(counts.values())

    # compute raw weights
    raw_weights = {
        cls: total / (3 * count)
        for cls, count in counts.items()
    }
    print("Raw class weights:", raw_weights)
    # map to model class index order
    class_weights = {
        0: raw_weights[-1],   # model class 0 = label -1
        1: raw_weights[0],    # model class 1 = label 0
        2: raw_weights[1]     # model class 2 = label 1
    }


    sample_weights = np.array([class_weights[label] for label in y_train])

    # TRAIN
    model.fit(X_train, y_train, sample_weight=sample_weights)

    # SAVE MODEL ARTIFACT
    model.save_model("xgb_multiclass.json")

    # ---------------------------------------------------------
    # PREDICT (multi-class probabilities)
    # ---------------------------------------------------------
    y_pred_model = model.predict(X_val)

    # MAP BACK TO TRADING LABELS
    inv_map = {0: -1, 1: 0, 2: 1}
    y_pred = np.vectorize(inv_map.get)(y_pred_model)
    y_val_trading = np.vectorize(inv_map.get)(y_val)

    # REPORT
    report = classification_report(y_val_trading, y_pred, output_dict=False)

    return report, label_stats, model