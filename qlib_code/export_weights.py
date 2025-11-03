import os
import argparse
import qlib
from qlib.workflow import R
from qlib.config import REG_CN
import pandas as pd
import logging

log = logging.getLogger(__name__)


def export_positions_and_bench(experiment_id, experiment_name, provider_uri=r"E:\qlib_data\tushare_qlib_data\qlib_bin", mlruns_uri="file:///E:/qlib_code/mlruns", output_dir=r"E:\qlib_data\exported_weights"):
   

    os.makedirs(output_dir, exist_ok=True)
    qlib.init(provider_uri=provider_uri, region=REG_CN)
    R.set_uri(mlruns_uri)
    recorder = R.get_recorder(experiment_id=experiment_id, experiment_name=experiment_name)

    out_exp_dir = os.path.join(output_dir, "test")
    os.makedirs(out_exp_dir, exist_ok=True)

    pos_artifact = "portfolio_analysis/positions_normal_1day.pkl"
    try:
        positions = recorder.load_object(pos_artifact)
       
        if isinstance(positions, dict):
            
                date_keys = list(positions.keys())
              
                try:
                    date_keys = sorted(date_keys, key=lambda x: pd.Timestamp(x))
                except Exception:
                    date_keys = list(date_keys)

                
                use_dates = date_keys


                for date_key in use_dates:
                    pos = positions[date_key]
                   
                    df = None
                    try:
                        df = pd.DataFrame(pos)
                    except Exception:
                        # If pos is a qlib Position-like object, it may expose get_stock_weight_dict
                        try:
                            if hasattr(pos, 'get_stock_weight_dict'):
                                wdict = pos.get_stock_weight_dict(only_stock=False)
                                # wdict is a dict{instrument: weight}
                                df = pd.DataFrame(list(wdict.items()), columns=['instrument', 'weight'])
                            elif hasattr(pos, 'to_dict'):
                                d = pos.to_dict()
                                # try to find mapping of instrument->weight inside
                                if isinstance(d, dict):
                                    # if values are scalars
                                    if all(not isinstance(v, (dict, list)) for v in d.values()):
                                        df = pd.DataFrame(list(d.items()), columns=['instrument', 'weight'])
                                    else:
                                        # try to normalize
                                        df = pd.json_normalize(d).T.reset_index()
                                        df.columns = ['instrument', 'weight']
                            else:
                                # try iterable of (instrument, weight)
                                try:
                                    items = list(pos)
                                    if items and isinstance(items[0], (list, tuple)) and len(items[0]) >= 2:
                                        df = pd.DataFrame(items, columns=['instrument', 'weight'])
                                except Exception:
                                    df = None
                        except Exception:
                            df = None
                    # fallback: if df still None, assign raw pos so later logic can try
                    if df is None:
                        df = pos
                    # try to standardize columns
                    if 'instrument' not in df.columns and 'symbol' in df.columns:
                        df = df.rename(columns={'symbol': 'instrument'})
                    if 'weight' not in df.columns:
                        # try to find weight-like column
                        for c in df.columns:
                            if 'weight' in c.lower() or 'w_' in c.lower():
                                df = df.rename(columns={c: 'weight'})
                                break
                    # Keep only instrument and weight
                    if 'instrument' in df.columns and 'weight' in df.columns:
                        out = df[['instrument', 'weight']].copy()
                    else:
                        out = df.copy()
                    # ensure filename uses safe YYYY-MM-DD
                    try:
                        safe_date = pd.Timestamp(date_key).strftime('%Y-%m-%d')
                    except Exception:
                        safe_date = str(date_key)
                    # if user requested scores, try to load preds for this date and merge
                    try:
                        preds_ser = _load_preds_for_date(recorder, date_key)
                        if preds_ser is not None and not preds_ser.empty:
                            try:
                                df_scores = preds_ser.reset_index()
                                # ensure column name
                                if df_scores.shape[1] >= 2:
                                    df_scores.columns = ['instrument', 'score']
                                else:
                                    df_scores.columns = ['instrument']
                                    df_scores['score'] = preds_ser.values
                                df_scores['instrument'] = df_scores['instrument'].astype(str)
                                # merge with out on instrument
                                out = out.merge(df_scores, on='instrument', how='left')
                            except Exception:
                                pass
                    except Exception:
                        pass

                    out_file = os.path.join(out_exp_dir, f"positions_{safe_date}.csv")
                    out.to_csv(out_file, index=False)
                    log.info(f"Saved positions for {safe_date} -> {out_file}")
        else:
            # positions is not a dict. Try to coerce single Position-like object
            df = None
            try:
                df = pd.DataFrame(positions)
            except Exception:
                try:
                    if hasattr(positions, 'get_stock_weight_dict'):
                        wdict = positions.get_stock_weight_dict(only_stock=False)
                        df = pd.DataFrame(list(wdict.items()), columns=['instrument', 'weight'])
                    elif hasattr(positions, 'to_dict'):
                        d = positions.to_dict()
                        if isinstance(d, dict):
                            if all(not isinstance(v, (dict, list)) for v in d.values()):
                                df = pd.DataFrame(list(d.items()), columns=['instrument', 'weight'])
                            else:
                                df = pd.json_normalize(d).T.reset_index()
                                df.columns = ['instrument', 'weight']
                    else:
                        try:
                            items = list(positions)
                            if items and isinstance(items[0], (list, tuple)) and len(items[0]) >= 2:
                                df = pd.DataFrame(items, columns=['instrument', 'weight'])
                        except Exception:
                            df = None
                except Exception:
                    df = None

            if df is None:
                out_file = os.path.join(out_exp_dir, "positions_unknown.txt")
                # save repr for debugging
                try:
                    with open(out_file, 'w', encoding='utf-8') as f:
                        f.write(repr(positions))
                    log.info(f"Saved positions (unknown format repr) -> {out_file}")
                except Exception as e:
                    log.exception(f"Failed to save unknown positions repr: {e}")
            else:
                out_file = os.path.join(out_exp_dir, "positions.csv")
                df.to_csv(out_file, index=False)
                log.info(f"Saved positions -> {out_file}")
    except Exception as e:
        log.exception(f"Could not load positions artifact '{pos_artifact}': {e}")

  

def _load_preds_for_date(recorder, date_key):
    """Load pred.pkl from recorder and return a Series mapping instrument->score for a given date_key."""
    try:
        preds = recorder.load_object('pred.pkl')
        # preds may be DataFrame with MultiIndex (date, instrument) or index of instruments and columns are dates
        # normalize into a Series with index (date, instrument) or (instrument,) depending on format
        if isinstance(preds, pd.DataFrame):
            # case 1: preds.index is MultiIndex with date level
            if isinstance(preds.index, pd.MultiIndex):
                # find rows where date level == date_key
                try:
                    date_ts = pd.Timestamp(date_key)
                    sel = preds.loc[date_ts]
                    # sel could be DataFrame (instruments x 1) or Series
                    if isinstance(sel, pd.DataFrame):
                        # try to pick the first column
                        ser = sel.iloc[:, 0]
                        ser.index = ser.index.astype(str)
                        return ser
                    else:
                        ser = pd.Series(sel)
                        ser.index = ser.index.astype(str)
                        return ser
                except Exception:
                    # fallback: try formatted date str
                    date_str = pd.Timestamp(date_key).strftime('%Y-%m-%d')
                    try:
                        sel = preds.xs(date_str, level=0, drop_level=False)
                        ser = sel.iloc[:, 0] if isinstance(sel, pd.DataFrame) else pd.Series(sel)
                        ser.index = ser.index.astype(str)
                        return ser
                    except Exception:
                        return None
            else:
              
                try:
                    date_str = pd.Timestamp(date_key).strftime('%Y-%m-%d')
                    if date_str in preds.columns:
                        ser = preds[date_str].dropna()
                        ser.index = ser.index.astype(str)
                        return ser
                    # try Timestamp
                    date_ts = pd.Timestamp(date_key)
                    # find column matching the timestamp
                    for c in preds.columns:
                        try:
                            if pd.Timestamp(c) == date_ts:
                                ser = preds[c].dropna()
                                ser.index = ser.index.astype(str)
                                return ser
                        except Exception:
                            continue
                except Exception:
                    return None
       
        return None
    except Exception:
        return None


if __name__ == '__main__':
    params = {
        'experiment_id': "894642627054463477",  
        'experiment_name': "test_lgb",  
        'provider_uri': r"E:\qlib_data\tushare_qlib_data\qlib_bin", 
        'mlruns_uri': "file:///E:/qlib_code/mlruns",  
        'output_dir': r"E:\qlib_data\exported_weights",  
    }
    
    export_positions_and_bench(**params)

