import requests
import json
import time
from decimal import Decimal, getcontext

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
SKIP_ID = 1001
ZERO_FEE_STOP_LIMIT = 100
ADDRESS_API = "https://sodex.dev/mainnet/chain/user/"
BASE_TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_address(uid):
    try:
        # Clean URL construction: account_id=1061 (no brackets)
        r = requests.get(f"{ADDRESS_API}{uid}/address", timeout=5).json()
        return r['data']['address'] if r.get('code') == 0 else None
    except: return None

def find_limit():
    low, high, limit = 1000, 20000, 1000
    while low <= high:
        mid = (low + high) // 2
        if get_address(mid):
            limit, low = mid, mid + 1
        else: high = mid - 1
    return limit

def main():
    upper_limit = find_limit()
    print(f"🎯 Range: {START_ID} to {upper_limit} | Skipping ID {SKIP_ID}", flush=True)
    
    # Prices
    prices = {}
    try:
        p_data = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        prices = {p['s']: Decimal(str(p['p'])) for p in p_data}
    except: pass

    results = {}
    streak_zeros = 0

    for uid in range(START_ID, upper_limit + 1):
        if uid == SKIP_ID:
            print(f"⏩ ID {uid} | Hard Skipped", flush=True)
            continue

        addr = get_address(uid)
        if not addr: continue

        print(f"📡 ID {uid} ", end="", flush=True)
        
        vol, fees, count = Decimal('0'), Decimal('0'), 0
        cursor = "" # Initialize empty cursor
        
        while True:
            # Construct URL: remove brackets, handle cursor
            url = f"{BASE_TRADE_URL}?account_id={uid}&limit=100"
            if cursor:
                url += f"&cursor={cursor}"
            
            try:
                resp = requests.get(url, timeout=10).json()
                trades = resp.get('data', [])
                
                if not trades: break
                
                count += len(trades)
                for t in trades:
                    p = Decimal(str(t.get('price', 1)))
                    vol += (Decimal(str(t.get('quantity', 0))) * p)
                    # Side 1 = Token Fee, Side 2 = USDC Fee
                    fees += (Decimal(str(t.get('fee', 0))) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t.get('fee', 0)))
                
                print(".", end="", flush=True)
                
                # Check for Meta Cursor
                meta = resp.get('meta', {})
                cursor = meta.get('next_cursor')
                
                # If no more pages (cursor is null or empty string), stop for this user
                if not cursor:
                    break
                    
                time.sleep(0.05) # Rate limit safety
            except Exception as e:
                print(f"⚠️", end="")
                break

        if count > 0:
            if fees == 0:
                streak_zeros += 1
                print(f" ⏩ Skip (Team Streak: {streak_zeros})", flush=True)
            else:
                streak_zeros = 0
                results[addr] = {"id": uid, "vol": float(round(vol, 2)), "fee": float(round(fees, 4))}
                print(f" ✅ Saved: ${round(vol, 2)}", flush=True)
        else:
            print(" 💨 No Trades", flush=True)

        if streak_zeros >= ZERO_FEE_STOP_LIMIT:
            print(f"🛑 Stopping: Hit {ZERO_FEE_STOP_LIMIT} zero-fee users.", flush=True)
            break

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"🏁 Done! Total users: {len(results)}")

if __name__ == "__main__":
    main()
