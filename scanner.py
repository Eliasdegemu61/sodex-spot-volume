import requests
import json
import time
from decimal import Decimal, getcontext
from concurrent.futures import ThreadPoolExecutor, as_completed

getcontext().prec = 50

# --- CONFIG ---
BASE_ID = 1000
MAX_THREADS = 10           # Higher number = Faster, but risks API ban
MAX_TRADES_PER_USER = 300000 
ADDRESS_URL = "https://sodex.dev/mainnet/chain/user/{}/address"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_market_prices():
    try:
        syms = requests.get("https://mainnet-gw.sodex.dev/bolt/symbols?names").json().get('data', [])
        id_map = {str(i['symbolID']): i['name'] for i in syms}
        prices = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        price_map = {p['s']: Decimal(str(p['p'])) for p in prices}
        return {s_id: price_map.get(name, Decimal('0')) for s_id, name in id_map.items()}
    except: return {}

def process_single_user(acc_id, price_map):
    """Worker function for a single thread."""
    try:
        resp = requests.get(ADDRESS_URL.format(acc_id)).json()
        if resp.get("code") != 0:
            return acc_id, None, None
            
        addr = resp["data"]["address"]
        vol, fees = Decimal('0'), Decimal('0')
        off, lim = 0, 100
        processed_count = 0
        
        while True:
            r = requests.get(f"{TRADE_URL}?account_id={acc_id}&limit={lim}&offset={off}").json()
            trades = r.get('data', [])
            if not trades: break
            
            processed_count += len(trades)
            if processed_count > MAX_TRADES_PER_USER:
                break # Stop at 300k trades

            for t in trades:
                p = price_map.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                side = int(t.get('side', 1))
                vol += (Decimal(str(t['quantity'])) * p)
                fees += (Decimal(str(t['fee'])) * p) if side == 1 else Decimal(str(t['fee']))
            
            off += lim
            if len(trades) < lim: break
            
        return acc_id, addr, {"id": acc_id, "vol": float(round(vol, 2)), "fee": float(round(fees, 4)), "ts": int(time.time())}
    except:
        return acc_id, None, None

def main():
    prices = get_market_prices()
    results = {}
    
    # 1. Discover all active IDs first
    active_ids = []
    print("🔍 Discovering active IDs...")
    curr_id = BASE_ID
    while True:
        # We check in small batches to find the endpoint
        r = requests.get(ADDRESS_URL.format(curr_id)).json()
        if r.get("code") == 404: break
        active_ids.append(curr_id)
        curr_id += 1
        if len(active_ids) % 50 == 0: print(f"Found {len(active_ids)} users...")

    # 2. Process users in parallel
    print(f"🚀 Starting Multi-threaded Scan ({MAX_THREADS} threads)...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_single_user, uid, prices): uid for uid in active_ids}
        
        for future in as_completed(futures):
            uid, addr, stats = future.result()
            if addr and stats:
                results[addr] = stats
                print(f"✅ {uid} processed.")

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"💾 Saved {len(results)} users to {OUT_FILE}")

if __name__ == "__main__":
    main()
