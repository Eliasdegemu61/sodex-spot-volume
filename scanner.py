import requests
import json
import time
from decimal import Decimal, getcontext

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
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

    print(f"🎯 UPPER LIMIT: {upper_limit}. Starting Deep Sync...", flush=True)

    for uid in range(START_ID, upper_limit + 1):
        addr = get_user_address_only(uid)
        if not addr: continue
        
        # --- Real-time "I am starting this user" log ---
        print(f"📡 Fetching ID {uid}...", end="", flush=True)
        
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
                
                # --- HEARTBEAT PRINT ---
                # This shows you it's still working on a "heavy" user
                print(".", end="", flush=True) 
                
                offset += limit
                if len(trades) < limit: break
            except: 
                print("⚠️", end="", flush=True)
                break

        # Final result for the user
        if trades_found > 0:
            if fees == 0:
                print(f" ⏩ Skip (Team)", flush=True)
            else:
                results[addr] = {"id": uid, "vol": float(round(vol, 2)), "fee": float(round(fees, 4)), "ts": int(time.time())}
                print(f" ✅ Vol: ${round(vol, 2)}", flush=True)
        else:
            print(" 💨 No trades", flush=True)

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print("🏁 Done!")

if __name__ == "__main__":
    main()
