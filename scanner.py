import requests
import json
import time
from decimal import Decimal, getcontext

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
ZERO_FEE_STOP_LIMIT = 100
ADDRESS_URL = "https://sodex.dev/mainnet/chain/user/{}/address"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_upper_limit():
    """Finds the actual highest User ID currently on the exchange."""
    print("🔍 Finding the current upper limit...", flush=True)
    high = START_ID
    step = 500
    # Jump forward until we hit a 404
    while True:
        try:
            r = requests.get(ADDRESS_URL.format(high + step), timeout=5).json()
            if r.get("code") == 0:
                high += step
            else:
                if step <= 1: break
                step //= 5 # Fine-tune the search
        except:
            break
    print(f"📈 Upper limit found: {high + 100} (adding buffer)", flush=True)
    return high + 100

def get_market_prices():
    try:
        syms = requests.get("https://mainnet-gw.sodex.dev/bolt/symbols?names").json().get('data', [])
        id_map = {str(i['symbolID']): i['name'] for i in syms}
        prices_data = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        price_map = {p['s']: Decimal(str(p['p'])) for p in prices_data}
        return {s_id: price_map.get(name, Decimal('0')) for s_id, name in id_map.items()}
    except: return {}

def main():
    # 1. Get Dynamic Limit
    UPPER_LIMIT = get_upper_limit()
    active_prices = get_market_prices()
    results = {}
    consecutive_zeros = 0
    
    print(f"🚀 Starting Scan | IDs {START_ID} to {UPPER_LIMIT}", flush=True)

    # 2. Sequential Scan
    for uid in range(START_ID, UPPER_LIMIT + 1):
        try:
            resp = requests.get(ADDRESS_URL.format(uid), timeout=10).json()
            if resp.get("code") != 0: continue 
            
            addr = resp["data"]["address"]
            vol, fees, trades_found = Decimal('0'), Decimal('0'), 0
            offset, limit = 0, 100
            
            while True:
                r = requests.get(f"{TRADE_URL}?account_id={uid}&limit={limit}&offset={offset}", timeout=15).json()
                trades = r.get('data', [])
                if not trades: break
                
                trades_found += len(trades)
                for t in trades:
                    p = active_prices.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                    vol += (Decimal(str(t['quantity'])) * p)
                    fees += (Decimal(str(t['fee'])) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t['fee']))
                
                offset += limit
                if len(trades) < limit: break
            
            if trades_found > 0:
                if fees == 0: consecutive_zeros += 1
                else: consecutive_zeros = 0
                
                results[addr] = {
                    "id": uid, "vol": float(round(vol, 2)), 
                    "fee": float(round(fees, 4)), "ts": int(time.time())
                }
                print(f"✅ User {uid} | Vol: ${round(vol, 2)} | Fees: ${round(fees, 4)}", flush=True)
            
            if consecutive_zeros >= ZERO_FEE_STOP_LIMIT:
                print(f"🛑 Stopped by fee circuit breaker at ID {uid}", flush=True)
                break

        except Exception as e:
            print(f"⚠️ Error at {uid}: {e}", flush=True)
            continue

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"🏁 Finished! Total Users: {len(results)}", flush=True)

if __name__ == "__main__":
    main()
