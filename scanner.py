import requests
import json
import time
from decimal import Decimal, getcontext
from concurrent.futures import ThreadPoolExecutor, as_completed

getcontext().prec = 50

# --- CONFIG ---
MAX_THREADS = 15           # Request 15 users at the same time
START_ID = 1000
SKIP_ID = 1001
ZERO_FEE_STOP_LIMIT = 100
ADDRESS_API = "https://sodex.dev/mainnet/chain/user/"
BASE_TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

# Use a Session for connection pooling (HUGE speed boost)
session = requests.Session()

def get_address(uid):
    try:
        r = session.get(f"{ADDRESS_API}{uid}/address", timeout=5).json()
        return r['data']['address'] if r.get('code') == 0 else None
    except: return None

def fetch_user_data(uid):
    """Worker function to fetch all trades for a single user"""
    if uid == SKIP_ID:
        return None, None

    addr = get_address(uid)
    if not addr:
        return None, None

    vol, fees, count = Decimal('0'), Decimal('0'), 0
    cursor = ""
    
    while True:
        url = f"{BASE_TRADE_URL}?account_id={uid}&limit=100"
        if cursor:
            url += f"&cursor={cursor}"
        
        try:
            resp = session.get(url, timeout=10).json()
            trades = resp.get('data', [])
            if not trades: break
            
            count += len(trades)
            for t in trades:
                p = Decimal(str(t.get('price', 1)))
                vol += (Decimal(str(t.get('quantity', 0))) * p)
                fees += (Decimal(str(t.get('fee', 0))) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t.get('fee', 0)))
            
            meta = resp.get('meta', {})
            cursor = meta.get('next_cursor')
            if not cursor: break
        except:
            break

    if count > 0:
        return addr, {"id": uid, "vol": float(round(vol, 2)), "fee": float(round(fees, 4))}
    return addr, "NO_TRADES"

def main():
    # 1. Binary search for limit (keep this sequential for accuracy)
    print("🔍 Hunting Upper Limit...", end="", flush=True)
    low, high, upper_limit = 1000, 20000, 1000
    while low <= high:
        mid = (low + high) // 2
        if get_address(mid):
            upper_limit, low = mid, mid + 1
        else: high = mid - 1
    print(f" Target: {upper_limit}", flush=True)

    results = {}
    streak_zeros = 0
    
    # 2. Multi-threaded processing
    print(f"🚀 Launching {MAX_THREADS} threads for IDs {START_ID} to {upper_limit}...", flush=True)
    
    # We process in small batches to respect the 'Circuit Breaker' (Zero-Fee streak)
    batch_size = 50
    for i in range(START_ID, upper_limit + 1, batch_size):
        ids_to_check = range(i, min(i + batch_size, upper_limit + 1))
        
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            future_to_id = {executor.submit(fetch_user_data, uid): uid for uid in ids_to_check}
            
            for future in as_completed(future_to_id):
                uid = future_to_id[future]
                try:
                    addr, data = future.result()
                    if data == "NO_TRADES":
                        print(f"💨 {uid}", end=" ", flush=True)
                    elif data:
                        if data["fee"] == 0:
                            streak_zeros += 1
                            print(f"🔘 {uid}", end=" ", flush=True)
                        else:
                            streak_zeros = 0
                            results[addr] = data
                            print(f"✅ {uid}", end=" ", flush=True)
                except Exception as e:
                    print(f"❌ {uid}", end=" ", flush=True)

        # Save progress every batch
        with open(OUT_FILE, "w") as f:
            json.dump(results, f, indent=4)
        
        if streak_zeros >= ZERO_FEE_STOP_LIMIT:
            print(f"\n🛑 Streak of {ZERO_FEE_STOP_LIMIT} zero-fee users reached. Stopping.")
            break

    print(f"\n🏁 Finished. Saved {len(results)} users.")

if __name__ == "__main__":
    main()
