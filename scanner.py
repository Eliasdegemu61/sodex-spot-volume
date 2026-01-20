import requests
import json
import time
import sys
from decimal import Decimal, getcontext

getcontext().prec = 50

# --- CONFIGURATION ---
START_ID = 1000
ADDRESS_API = "https://sodex.dev/mainnet/chain/user/"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_user_address_only(user_id):
    """Checks if ID exists and returns address (Matches your Google Script logic)"""
    try:
        url = f"{ADDRESS_API}{user_id}/address"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 0 and data.get("data") and data["data"].get("address"):
                return data["data"]["address"]
    except:
        pass
    return None

def find_upper_limit():
    """Exact translation of your Google Script Binary Search"""
    print("🔍 [STEP 1] HUNTING UPPER LIMIT...", flush=True)
    low = 1000
    high = 20000  # Increased range just in case
    upper_limit = 1000
    
    while low <= high:
        mid = (low + high) // 2
        address = get_user_address_only(mid)
        
        if address:
            upper_limit = mid
            low = mid + 1
        else:
            high = mid - 1
        time.sleep(0.06) # Your API_DELAY
    
    print(f"🎯 UPPER LIMIT FOUND: {upper_limit}", flush=True)
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

    print(f"🚀 [STEP 2] STARTING REAL-TIME TRACKING (IDs {START_ID} to {upper_limit})", flush=True)
    print("-" * 50, flush=True)

    for uid in range(START_ID, upper_limit + 1):
        try:
            addr = get_user_address_only(uid)
            if not addr:
                continue
            
            vol, fees, trades_found = Decimal('0'), Decimal('0'), 0
            offset, limit = 0, 100
            
            while True:
                r = requests.get(f"{TRADE_URL}?account_id={uid}&limit={limit}&offset={offset}", timeout=10).json()
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
                # SKIP TEAM ADDRESSES
                if fees == 0:
                    print(f"⏩ SKIP | ID: {uid} | Addr: {addr[:10]}... | Reason: Team/Zero Fee", flush=True)
                    continue
                
                results[addr] = {
                    "id": uid, "vol": float(round(vol, 2)), 
                    "fee": float(round(fees, 4)), "ts": int(time.time())
                }
                # This is what you'll see live in GitHub
                print(f"✅ TRACKING | ID: {uid} | Vol: ${round(vol, 2)} | Fees: ${round(fees, 4)}", flush=True)

        except Exception as e:
            print(f"⚠️ Error on ID {uid}: {e}", flush=True)

    # Final Save
    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print("-" * 50, flush=True)
    print(f"🏁 DONE! Total real addresses tracked: {len(results)}", flush=True)

if __name__ == "__main__":
    main()
