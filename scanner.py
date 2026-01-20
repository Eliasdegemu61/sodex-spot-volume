import requests
import json
import time
from decimal import Decimal, getcontext
from concurrent.futures import ThreadPoolExecutor

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
END_ID = 8000               # Increased limit since we now have a circuit breaker
MAX_THREADS = 10
ZERO_FEE_STOP_LIMIT = 100   # Stop if 100 consecutive users have 0 fees
ADDRESS_URL = "https://sodex.dev/mainnet/chain/user/{}/address"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

# Global counter to track consecutive zero-fee users across threads
class GlobalTracker:
    def __init__(self):
        self.consecutive_zeros = 0
        self.stop_signal = False

tracker = GlobalTracker()

def get_market_prices():
    try:
        syms = requests.get("https://mainnet-gw.sodex.dev/bolt/symbols?names").json().get('data', [])
        id_map = {str(i['symbolID']): i['name'] for i in syms}
        prices = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        price_map = {p['s']: Decimal(str(p['p'])) for p in prices}
        return {s_id: price_map.get(name, Decimal('0')) for s_id, name in id_map.items()}
    except: return {}

def process_id(uid, price_map):
    if tracker.stop_signal: return None

    try:
        resp = requests.get(ADDRESS_URL.format(uid), timeout=5).json()
        if resp.get("code") != 0: return "SKIP"
            
        addr = resp["data"]["address"]
        vol, fees, trades_found = Decimal('0'), Decimal('0'), 0
        offset, limit = 0, 100
        
        while True:
            r = requests.get(f"{TRADE_URL}?account_id={uid}&limit={limit}&offset={offset}", timeout=10).json()
            trades = r.get('data', [])
            if not trades: break
            
            trades_found += len(trades)
            for t in trades:
                p = price_map.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                vol += (Decimal(str(t['quantity'])) * p)
                fees += (Decimal(str(t['fee'])) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t['fee']))
            
            offset += limit
            if len(trades) < limit: break
            
        # --- CIRCUIT BREAKER LOGIC ---
        if fees == 0:
            tracker.consecutive_zeros += 1
            if tracker.consecutive_zeros >= ZERO_FEE_STOP_LIMIT:
                print(f"🛑 CIRCUIT BREAKER: {ZERO_FEE_STOP_LIMIT} users with 0 fees. Stopping.")
                tracker.stop_signal = True
        else:
            tracker.consecutive_zeros = 0 # Reset streak if we find a real payer

        if trades_found > 0:
            return {"addr": addr, "id": uid, "vol": float(round(vol, 2)), "fee": float(round(fees, 4))}
            
    except: pass
    return None

def main():
    prices = get_market_prices()
    final_results = {}
    
    print(f"🚀 Scanning IDs {START_ID} to {END_ID} with 0-fee Circuit Breaker...")
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Process sequentially in small batches of 10 to check the circuit breaker regularly
        for i in range(START_ID, END_ID + 1, MAX_THREADS):
            if tracker.stop_signal: break
            
            batch = range(i, min(i + MAX_THREADS, END_ID + 1))
            futures = [executor.submit(process_id, uid, prices) for uid in batch]
            
            for f in futures:
                res = f.result()
                if res and res != "SKIP":
                    final_results[res["addr"]] = {"id": res["id"], "vol": res["vol"], "fee": res["fee"], "ts": int(time.time())}
                    print(f"✅ {res['id']} | Fees: ${res['fee']}")

    with open(OUT_FILE, "w") as f:
        json.dump(final_results, f, indent=4)
    print(f"💾 Saved {len(final_results)} users. Scan ended at ID {tracker.consecutive_zeros + START_ID if tracker.stop_signal else END_ID}")

if __name__ == "__main__":
    main()
