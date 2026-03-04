"""
=====================================================
  VN30 BACKTEST ENGINE — SMA Crossover + RSI Filter
  Chiến lược: Mua khi SMA_short cắt lên SMA_long
              Bán khi SMA_short cắt xuống SMA_long
              Lọc nhiễu bằng RSI
=====================================================
"""

import os, json, time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from vnstock import stock_historical_data
except ImportError:
    raise ImportError("pip install vnstock")

# ─────────────────────────────────────────
# CẤU HÌNH BACKTEST
# ─────────────────────────────────────────
CONFIG = {
    # Danh mục
    "tickers": [
        'ACB','BCM','BID','BVH','CTG','FPT','GAS','GVR','HDB','HPG',
        'MBB','MSN','MWG','PLX','POW','SAB','SHB','SSB','SSI','STB',
        'TCB','TPB','VCB','VHM','VIB','VIC','VJC','VNM','VPB','VRE'
    ],
    # Khoảng thời gian
    "start_date": "2020-01-01",
    "end_date":   datetime.now().strftime("%Y-%m-%d"),
    # Tham số chiến lược
    "short_window": 20,
    "long_window":  50,
    "rsi_period":   14,
    "rsi_ob":       70,      # Overbought — lọc tín hiệu mua
    "rsi_os":       30,      # Oversold   — lọc tín hiệu bán
    # Quản lý vốn
    "initial_capital":    100_000_000,   # 100 triệu VNĐ
    "position_size_pct":  10,            # Mỗi lệnh dùng 10% vốn
    "commission_pct":     0.15,          # Phí 0.15% mỗi chiều
    "slippage_pct":       0.05,          # Trượt giá 0.05%
    # Quản lý rủi ro
    "stop_loss_pct":      5.0,           # Cắt lỗ 5%
    "take_profit_pct":    15.0,          # Chốt lời 15%
    "max_hold_days":      60,            # Giữ tối đa 60 ngày
    # Khác
    "max_workers": 8,
}

# ─────────────────────────────────────────
# CHỈ BÁO KỸ THUẬT
# ─────────────────────────────────────────
def calc_sma(s, w):      return s.rolling(w).mean()
def calc_ema(s, w):      return s.ewm(span=w, adjust=False).mean()

def calc_rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    return 100 - 100/(1 + g/l)

def calc_atr(df, p=14):
    hl = df['high'] - df['low']
    hc = (df['high'] - df['close'].shift()).abs()
    lc = (df['low']  - df['close'].shift()).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(p).mean()

def add_indicators(df, cfg):
    c = df['close']
    df['sma_s']  = calc_sma(c, cfg['short_window'])
    df['sma_l']  = calc_sma(c, cfg['long_window'])
    df['rsi']    = calc_rsi(c, cfg['rsi_period'])
    df['atr']    = calc_atr(df)
    df['vol_ma'] = df['volume'].rolling(20).mean()
    # Tín hiệu giao cắt
    df['cross_up']   = (df['sma_s'] > df['sma_l']) & (df['sma_s'].shift() <= df['sma_l'].shift())
    df['cross_down'] = (df['sma_s'] < df['sma_l']) & (df['sma_s'].shift() >= df['sma_l'].shift())
    # Xác nhận xu hướng (nến -3)
    df['confirm_up']   = df['sma_s'].shift(2) <= df['sma_l'].shift(2)
    df['confirm_down'] = df['sma_s'].shift(2) >= df['sma_l'].shift(2)
    return df.dropna()

# ─────────────────────────────────────────
# ENGINE BACKTEST CHO 1 MÃ
# ─────────────────────────────────────────
def backtest_ticker(ticker, df, cfg):
    capital    = cfg['initial_capital']
    pos_size   = cfg['position_size_pct'] / 100
    comm       = cfg['commission_pct']    / 100
    slip       = cfg['slippage_pct']      / 100
    sl_pct     = cfg['stop_loss_pct']     / 100
    tp_pct     = cfg['take_profit_pct']   / 100
    max_hold   = cfg['max_hold_days']

    trades     = []
    position   = None   # {"entry_price", "shares", "entry_date", "entry_idx"}
    equity_curve = []

    for i in range(len(df)):
        row    = df.iloc[i]
        price  = row['close']
        date   = df.index[i]

        # ── QUẢN LÝ VỊ THẾ ĐANG GIỮ ──
        if position:
            hold_days = (date - position['entry_date']).days
            pnl_pct   = (price - position['entry_price']) / position['entry_price']

            exit_reason = None
            exit_price  = price

            if pnl_pct <= -sl_pct:
                exit_reason = "STOP_LOSS"
                exit_price  = position['entry_price'] * (1 - sl_pct)
            elif pnl_pct >= tp_pct:
                exit_reason = "TAKE_PROFIT"
                exit_price  = position['entry_price'] * (1 + tp_pct)
            elif hold_days >= max_hold:
                exit_reason = "MAX_HOLD"
            elif row['cross_down'] and row['confirm_down'] and row['rsi'] > cfg['rsi_os']:
                exit_reason = "SIGNAL_SELL"

            if exit_reason:
                exit_price_slip = exit_price * (1 - slip)
                gross_pnl = (exit_price_slip - position['entry_price']) * position['shares']
                comm_cost = exit_price_slip * position['shares'] * comm
                net_pnl   = gross_pnl - comm_cost
                capital  += position['cost'] + net_pnl

                trades.append({
                    "ticker":      ticker,
                    "entry_date":  position['entry_date'],
                    "exit_date":   date,
                    "entry_price": round(position['entry_price'], 0),
                    "exit_price":  round(exit_price_slip, 0),
                    "shares":      position['shares'],
                    "gross_pnl":   round(gross_pnl, 0),
                    "net_pnl":     round(net_pnl, 0),
                    "pnl_pct":     round(pnl_pct * 100, 2),
                    "hold_days":   hold_days,
                    "exit_reason": exit_reason,
                    "rsi_entry":   round(position['rsi_entry'], 1),
                    "rsi_exit":    round(row['rsi'], 1),
                })
                position = None

        # ── TÍN HIỆU VÀO LỆNH ──
        if (position is None
                and row['cross_up']
                and row['confirm_up']
                and row['rsi'] < cfg['rsi_ob']):

            entry_price = price * (1 + slip)
            invest      = capital * pos_size
            shares      = int(invest / entry_price / 100) * 100   # Làm tròn lô 100
            if shares < 100:
                equity_curve.append({"date": date, "equity": capital})
                continue

            cost        = entry_price * shares
            comm_cost   = cost * comm
            total_cost  = cost + comm_cost

            if total_cost <= capital:
                capital -= total_cost
                position = {
                    "entry_price": entry_price,
                    "shares":      shares,
                    "cost":        total_cost,
                    "entry_date":  date,
                    "rsi_entry":   row['rsi'],
                }

        # Mark-to-market equity
        mtm = capital
        if position:
            mtm += price * position['shares']
        equity_curve.append({"date": date, "equity": mtm})

    # Đóng lệnh cuối nếu còn
    if position and len(df) > 0:
        last_price = df.iloc[-1]['close'] * (1 - slip)
        gross_pnl  = (last_price - position['entry_price']) * position['shares']
        comm_cost  = last_price * position['shares'] * comm
        net_pnl    = gross_pnl - comm_cost
        capital   += position['cost'] + net_pnl
        trades.append({
            "ticker":      ticker,
            "entry_date":  position['entry_date'],
            "exit_date":   df.index[-1],
            "entry_price": round(position['entry_price'], 0),
            "exit_price":  round(last_price, 0),
            "shares":      position['shares'],
            "gross_pnl":   round(gross_pnl, 0),
            "net_pnl":     round(net_pnl, 0),
            "pnl_pct":     round((last_price/position['entry_price']-1)*100, 2),
            "hold_days":   (df.index[-1]-position['entry_date']).days,
            "exit_reason": "END_OF_DATA",
            "rsi_entry":   round(position['rsi_entry'], 1),
            "rsi_exit":    round(df.iloc[-1]['rsi'], 1),
        })

    return trades, pd.DataFrame(equity_curve).set_index("date") if equity_curve else pd.DataFrame()

# ─────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────
def calc_metrics(trades_df, equity_df, initial_capital):
    if trades_df.empty:
        return {}

    total_trades  = len(trades_df)
    wins          = trades_df[trades_df['net_pnl'] > 0]
    losses        = trades_df[trades_df['net_pnl'] <= 0]
    win_rate      = len(wins) / total_trades * 100
    avg_win       = wins['net_pnl'].mean()   if len(wins)   else 0
    avg_loss      = losses['net_pnl'].mean() if len(losses) else 0
    profit_factor = abs(wins['net_pnl'].sum() / losses['net_pnl'].sum()) if losses['net_pnl'].sum() != 0 else float('inf')
    total_pnl     = trades_df['net_pnl'].sum()
    total_return  = total_pnl / initial_capital * 100

    # Sharpe (annualized, daily returns từ equity curve)
    sharpe = 0.0
    max_dd = 0.0
    if not equity_df.empty and 'equity' in equity_df.columns:
        eq     = equity_df['equity']
        daily  = eq.pct_change().dropna()
        if daily.std() > 0:
            sharpe = (daily.mean() / daily.std()) * np.sqrt(252)
        roll_max = eq.cummax()
        dd       = (eq - roll_max) / roll_max * 100
        max_dd   = dd.min()

    # Thời gian giao dịch
    years = (trades_df['exit_date'].max() - trades_df['entry_date'].min()).days / 365 if total_trades else 1
    cagr  = ((1 + total_return/100) ** (1/max(years,1)) - 1) * 100 if years > 0 else 0

    # Phân loại lý do thoát
    exit_counts = trades_df['exit_reason'].value_counts().to_dict()

    return {
        "total_trades":   total_trades,
        "win_rate":       round(win_rate, 1),
        "avg_win":        round(avg_win, 0),
        "avg_loss":       round(avg_loss, 0),
        "profit_factor":  round(profit_factor, 2),
        "total_pnl":      round(total_pnl, 0),
        "total_return":   round(total_return, 2),
        "cagr":           round(cagr, 2),
        "sharpe":         round(sharpe, 2),
        "max_drawdown":   round(max_dd, 2),
        "avg_hold_days":  round(trades_df['hold_days'].mean(), 1),
        "exit_breakdown": exit_counts,
        "years_tested":   round(years, 1),
    }

# ─────────────────────────────────────────
# FETCH DATA
# ─────────────────────────────────────────
def fetch_ticker(ticker, start, end, retries=3):
    for attempt in range(retries):
        try:
            df = stock_historical_data(
                symbol=ticker, start_date=start, end_date=end,
                resolution='1D', type='stock', beautify=True)
            if df is not None and not df.empty:
                df.index = pd.to_datetime(df.index)
                df = df.sort_index()
                for col in ['open','high','low','close','volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                return df.dropna(subset=['close'])
        except Exception as e:
            if attempt == retries-1:
                print(f"  ⚠️  {ticker}: lỗi fetch — {e}")
            time.sleep(1.5*(attempt+1))
    return None

# ─────────────────────────────────────────
# CHẠY BACKTEST TOÀN BỘ VN30
# ─────────────────────────────────────────
def run_backtest(cfg):
    print("═"*60)
    print(f"  📊 VN30 BACKTEST ENGINE")
    print(f"  Chiến lược : SMA{cfg['short_window']}/{cfg['long_window']} + RSI{cfg['rsi_period']}")
    print(f"  Thời gian  : {cfg['start_date']} → {cfg['end_date']}")
    print(f"  Vốn ban đầu: {cfg['initial_capital']:,.0f} VNĐ")
    print(f"  Cắt lỗ / Chốt lời: {cfg['stop_loss_pct']}% / {cfg['take_profit_pct']}%")
    print("═"*60)

    all_trades    = []
    all_equity    = {}
    ticker_summary = []

    def process(ticker):
        df_raw = fetch_ticker(ticker, cfg['start_date'], cfg['end_date'])
        if df_raw is None or len(df_raw) < cfg['long_window'] + 5:
            print(f"  ⚠️  {ticker}: không đủ dữ liệu")
            return None
        df = add_indicators(df_raw.copy(), cfg)
        trades, equity = backtest_ticker(ticker, df, cfg)
        return ticker, trades, equity

    print(f"\n  Đang tải dữ liệu {len(cfg['tickers'])} mã...\n")
    with ThreadPoolExecutor(max_workers=cfg['max_workers']) as ex:
        futures = {ex.submit(process, t): t for t in cfg['tickers']}
        for fut in as_completed(futures):
            res = fut.result()
            if res is None: continue
            ticker, trades, equity = res
            if trades:
                t_df = pd.DataFrame(trades)
                m    = calc_metrics(t_df, equity, cfg['initial_capital'])
                ticker_summary.append({"ticker": ticker, **m})
                all_trades.extend(trades)
                if not equity.empty:
                    all_equity[ticker] = equity['equity']
                print(f"  ✅ {ticker:<6} | {m['total_trades']:>3} lệnh | "
                      f"WR {m['win_rate']:>5.1f}% | "
                      f"PnL {m['total_return']:>+7.2f}% | "
                      f"Sharpe {m['sharpe']:>5.2f} | "
                      f"MaxDD {m['max_drawdown']:>7.2f}%")
            else:
                print(f"  ⚪ {ticker:<6} | Không có tín hiệu")

    return all_trades, all_equity, ticker_summary

# ─────────────────────────────────────────
# IN BÁO CÁO TỔNG HỢP
# ─────────────────────────────────────────
def print_summary(all_trades, ticker_summary, cfg):
    if not all_trades:
        print("\n❌ Không có giao dịch nào được thực hiện.")
        return

    all_df = pd.DataFrame(all_trades)
    all_eq_values = pd.DataFrame(ticker_summary)

    print("\n" + "═"*60)
    print("  📈 KẾT QUẢ TỔNG HỢP VN30")
    print("═"*60)

    # --- Top 5 mã tốt nhất ---
    if not all_eq_values.empty and 'total_return' in all_eq_values.columns:
        top5 = all_eq_values.nlargest(5, 'total_return')[
            ['ticker','total_trades','win_rate','total_return','sharpe','max_drawdown']
        ]
        print("\n  🏆 TOP 5 MÃ HIỆU QUẢ NHẤT:")
        print(f"  {'MÃ':<6} {'LỆNH':>5} {'WIN%':>7} {'RETURN%':>9} {'SHARPE':>8} {'MAX_DD%':>9}")
        print("  " + "─"*50)
        for _, r in top5.iterrows():
            print(f"  {r['ticker']:<6} {int(r['total_trades']):>5} {r['win_rate']:>7.1f}%"
                  f" {r['total_return']:>+9.2f}% {r['sharpe']:>8.2f} {r['max_drawdown']:>9.2f}%")

    # --- Thống kê toàn bộ ---
    wins   = all_df[all_df['net_pnl'] > 0]
    losses = all_df[all_df['net_pnl'] <= 0]
    pf     = abs(wins['net_pnl'].sum()/losses['net_pnl'].sum()) if len(losses) and losses['net_pnl'].sum()!=0 else float('inf')

    print(f"\n  📌 THỐNG KÊ TỔNG:")
    print(f"  Tổng giao dịch  : {len(all_df)}")
    print(f"  Tỷ lệ thắng     : {len(wins)/len(all_df)*100:.1f}%")
    print(f"  Profit Factor   : {pf:.2f}")
    print(f"  TB ngày nắm giữ : {all_df['hold_days'].mean():.1f} ngày")
    print(f"  TB lãi/lệnh     : {wins['net_pnl'].mean():,.0f} VNĐ" if len(wins) else "")
    print(f"  TB lỗ/lệnh      : {losses['net_pnl'].mean():,.0f} VNĐ" if len(losses) else "")

    # --- Lý do thoát ---
    print(f"\n  🚪 LÝ DO THOÁT LỆNH:")
    ec = all_df['exit_reason'].value_counts()
    for reason, count in ec.items():
        pct = count/len(all_df)*100
        bar = "█" * int(pct/4)
        print(f"  {reason:<14}: {count:>4} ({pct:>5.1f}%) {bar}")

    # --- Phân phối lợi nhuận ---
    print(f"\n  💰 PHÂN PHỐI LỢI NHUẬN (%):")
    bins = [(-999,-15),(-15,-10),(-10,-5),(-5,0),(0,5),(5,10),(10,15),(15,999)]
    labels = ["< -15%","-15→-10%","-10→-5%","-5→0%","0→5%","5→10%","10→15%","> 15%"]
    for (lo,hi), label in zip(bins, labels):
        cnt = ((all_df['pnl_pct']>lo)&(all_df['pnl_pct']<=hi)).sum()
        bar = "█" * cnt
        print(f"  {label:<12}: {cnt:>3}  {bar}")

    print("\n" + "═"*60)

# ─────────────────────────────────────────
# XUẤT KẾT QUẢ RA FILE
# ─────────────────────────────────────────
def export_results(all_trades, ticker_summary, cfg):
    os.makedirs("backtest_results", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Chi tiết từng lệnh
    if all_trades:
        trades_df = pd.DataFrame(all_trades)
        trades_df['entry_date'] = trades_df['entry_date'].astype(str)
        trades_df['exit_date']  = trades_df['exit_date'].astype(str)
        trades_path = f"backtest_results/trades_{ts}.csv"
        trades_df.to_csv(trades_path, index=False, encoding='utf-8-sig')
        print(f"\n  💾 Đã lưu chi tiết lệnh: {trades_path}")

    # Tóm tắt theo mã
    if ticker_summary:
        summary_df = pd.DataFrame(ticker_summary)
        summary_path = f"backtest_results/summary_{ts}.csv"
        summary_df.to_csv(summary_path, index=False, encoding='utf-8-sig')
        print(f"  💾 Đã lưu tóm tắt mã   : {summary_path}")

    # Config
    cfg_path = f"backtest_results/config_{ts}.json"
    cfg_copy = {k: v for k,v in cfg.items() if k != 'tickers'}
    with open(cfg_path, 'w', encoding='utf-8') as f:
        json.dump(cfg_copy, f, ensure_ascii=False, indent=2)
    print(f"  💾 Đã lưu cấu hình      : {cfg_path}")

    return trades_path if all_trades else None

# ─────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description="VN30 Backtest Engine")
    ap.add_argument("--start",    default=CONFIG["start_date"],  help="Ngày bắt đầu YYYY-MM-DD")
    ap.add_argument("--end",      default=CONFIG["end_date"],    help="Ngày kết thúc YYYY-MM-DD")
    ap.add_argument("--sma-short",type=int, default=CONFIG["short_window"])
    ap.add_argument("--sma-long", type=int, default=CONFIG["long_window"])
    ap.add_argument("--sl",       type=float, default=CONFIG["stop_loss_pct"],   help="Stop loss %")
    ap.add_argument("--tp",       type=float, default=CONFIG["take_profit_pct"], help="Take profit %")
    ap.add_argument("--capital",  type=float, default=CONFIG["initial_capital"])
    ap.add_argument("--tickers",  type=str, default=None, help="Danh sách mã, VD: FPT,HPG,VNM")
    ap.add_argument("--no-export",action="store_true", help="Không lưu file CSV")
    args = ap.parse_args()

    cfg = CONFIG.copy()
    cfg["start_date"]     = args.start
    cfg["end_date"]       = args.end
    cfg["short_window"]   = args.sma_short
    cfg["long_window"]    = args.sma_long
    cfg["stop_loss_pct"]  = args.sl
    cfg["take_profit_pct"]= args.tp
    cfg["initial_capital"]= args.capital
    if args.tickers:
        cfg["tickers"] = [t.strip().upper() for t in args.tickers.split(",")]

    t_start = time.time()
    all_trades, all_equity, ticker_summary = run_backtest(cfg)
    print_summary(all_trades, ticker_summary, cfg)
    if not args.no_export:
        export_results(all_trades, ticker_summary, cfg)
    print(f"\n  ⏱  Tổng thời gian chạy: {time.time()-t_start:.1f}s")