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
    """Translation of your getUserAddressOnly function"""
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
    """Translation of your findUpperLimit Binary Search logic"""
    print("🔍 Finding upper limit via Binary Search...", flush=True)
    low = 1000
    high = 10000 # Your Google Script used 10000
    upper_limit = 1000
    
    while low <= high:
        mid = (low + high) // 2
        address = get_user_address_only(mid)
        
        if address:
            upper_limit = mid
            low = mid + 1
        else:
            high = mid - 1
            
        # Small delay to mimic your API_DELAY
        time.sleep(0.06) 
    
    print(f"✅ Found upper limit: {upper_limit}", flush=True)
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
    # 1. Start exactly like your Google Script
    upper_limit = find_upper_limit()
    prices = get_market_prices()
    results = {}

    print(f"📍 Processing IDs 1000 to {upper_limit}...", flush=True)

    # 2. Linear processing (One-by-one as you requested)
    for uid in range(START_ID, upper_limit + 1):
        try:
            addr = get_user_address_only(uid)
            if not addr:
                continue
            
            vol, fees, trades_found = Decimal('0'), Decimal('0'), 0
            offset, limit = 0, 100
            
            # Fetch trades
            while True:
                r = requests.get(f"{TRADE_URL}?account_id={uid}&limit={limit}&offset={offset}", timeout=10).json()
                trades = r.get('data', [])
                if not trades: break
                
                trades_found += len(trades)
                for t in trades:
                    p = prices.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                    vol += (Decimal(str(t['quantity'])) * p)
                    fees += (Decimal(str(t['fee'])) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t['fee']))
                
                offset += limit
                if len(trades) < limit: break
            
            if trades_found > 0:
                # SKIP logic for Team addresses ($0 fees)
                if fees == 0:
                    print(f"⏩ ID {uid} skipped (Zero Fees)", flush=True)
                    continue
                    
                results[addr] = {
                    "id": uid, "vol": float(round(vol, 2)), 
                    "fee": float(round(fees, 4)), "ts": int(time.time())
                }
                print(f"✅ ID {uid} | Vol: ${round(vol, 2)} | Fees: ${round(fees, 4)}", flush=True)

        except Exception as e:
            print(f"⚠️ Error at {uid}: {e}", flush=True)

    # 3. Final Save
    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"🏁 DONE! Total users: {len(results)}", flush=True)

if __name__ == "__main__":
    main()
