import requests
import json
import time
from decimal import Decimal, getcontext

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
SKIP_ID = 1001             # Always skip this ID
ZERO_FEE_STOP_LIMIT = 100  # Stop script if 100 users in a row have 0 fees
ADDRESS_API = "https://sodex.dev/mainnet/chain/user/"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_user_address_only(user_id):
    try:
        url = f"{ADDRESS_API}{user_id}/address"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 0 and data.get("data") and data["data"].get("address"):
                return data["data"]["address"]
    except: pass
    return None

def find_upper_limit():
    low, high, upper_limit = 1000, 20000, 1000
    while low <= high:
        mid = (low + high) // 2
        if get_user_address_only(mid):
            upper_limit, low = mid, mid + 1
        else:
            high = mid - 1
    return upper_limit

def get_market_prices():
    try:
        syms = requests.get("https://mainnet-gw.sodex.dev/bolt/symbols?names").json().get('data', [])
        id_map = {str(i['symbolID']): i['name'] for i in syms}
        prices_data = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        price_map = {p['s']: Decimal(str(p['p'])) for p in prices_data}
        return {s_id: price_map.get(name, Decimal('0')) for s_id, name in id_map.items()}
    except: return {}

def main():
    upper_limit = find_upper_limit()
    active_prices = get_market_prices()
    results = {}
    consecutive_zeros = 0 # Tracker for the circuit breaker

    print(f"🎯 UPPER LIMIT: {upper_limit}. Syncing IDs {START_ID} to {upper_limit}...", flush=True)

    for uid in range(START_ID, upper_limit + 1):
        # 1. Hard Skip for 1001
        if uid == SKIP_ID:
            print(f"⏩ ID {uid} | Hard Skipped (Config)", flush=True)
            continue

        addr = get_user_address_only(uid)
        if not addr: continue
        
        print(f"📡 ID {uid} ", end="", flush=True)
        
        vol, fees, trades_found, offset, limit = Decimal('0'), Decimal('0'), 0, 0, 100
        
        while True:
            try:
                r = requests.get(f"{TRADE_URL}?account_id={uid}&limit={limit}&offset={offset}", timeout=10).json()
                trades = r.get('data', [])
                if not trades: break
                
                trades_found += len(trades)
                for t in trades:
                    p = active_prices.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                    vol += (Decimal(str(t['quantity'])) * p)
                    fees += (Decimal(str(t['fee'])) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t['fee']))
                
                print(".", end="", flush=True) # Heartbeat: 1 dot per 100 trades
                offset += limit
                if len(trades) < limit: break
            except: 
                print("⚠️", end="", flush=True)
                break

        # 2. Logic for filtering and Circuit Breaker
        if trades_found > 0:
            if fees == 0:
                consecutive_zeros += 1
                print(f" ⏩ Skip (Zero Fees) [Streak: {consecutive_zeros}]", flush=True)
            else:
                consecutive_zeros = 0 # Reset streak because we found a real payer
                results[addr] = {"id": uid, "vol": float(round(vol, 2)), "fee": float(round(fees, 4)), "ts": int(time.time())}
                print(f" ✅ Saved | Fees: ${round(fees, 4)}", flush=True)
        else:
            print(" 💨 No Trades", flush=True)

        # 3. Trigger Circuit Breaker
        if consecutive_zeros >= ZERO_FEE_STOP_LIMIT:
            print(f"\n🛑 CIRCUIT BREAKER: {ZERO_FEE_STOP_LIMIT} zero-fee users in a row. Ending script.", flush=True)
            break

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"🏁 Done! Total users saved: {len(results)}")

if __name__ == "__main__":
    main()
