import requests
import json
import time
from decimal import Decimal, getcontext

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
END_ID = 6000               # Wide range to catch everyone
CHUNK_SIZE = 100            # Save progress every 100 IDs
ZERO_FEE_STOP_LIMIT = 100   # Your "Team Address" trick
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

def main():
    prices = get_market_prices()
    results = {}
    consecutive_zeros = 0
    
    print(f"🚀 Starting RELIABLE Scan (1 Thread) | IDs {START_ID} to {END_ID}")

    for uid in range(START_ID, END_ID + 1):
        try:
            # 1. Check if ID exists
            resp = requests.get(ADDRESS_URL.format(uid), timeout=10).json()
            if resp.get("code") != 0:
                continue 
            
            addr = resp["data"]["address"]
            vol, fees, trades_found = Decimal('0'), Decimal('0'), 0
            offset, limit = 0, 100
            
            # 2. Process ALL trades for this specific ID
            while True:
                r = requests.get(f"{TRADE_URL}?account_id={uid}&limit={limit}&offset={offset}", timeout=15).json()
                trades = r.get('data', [])
                if not trades: break
                
                trades_found += len(trades)
                for t in trades:
                    p = price_map.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                    vol += (Decimal(str(t['quantity'])) * p)
                    fees += (Decimal(str(t['fee'])) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t['fee']))
                
                offset += limit
                if len(trades) < limit: break
            
            # --- THE TRICK: Fee Logic ---
            if trades_found > 0:
                if fees == 0:
                    consecutive_zeros += 1
                else:
                    consecutive_zeros = 0 # Reset streak if they paid fees
                
                results[addr] = {
                    "id": uid, "vol": float(round(vol, 2)), 
                    "fee": float(round(fees, 4)), "ts": int(time.time())
                }
                print(f"✅ Found User {uid} | Vol: ${round(vol, 2)} | Fees: ${round(fees, 4)}")
            
            # --- CIRCUIT BREAKER ---
            if consecutive_zeros >= ZERO_FEE_STOP_LIMIT:
                print(f"🛑 Stopping: Hit {ZERO_FEE_STOP_LIMIT} users with 0 fees.")
                break

            # 3. Save progress in chunks so we don't lose data if GitHub crashes
            if uid % CHUNK_SIZE == 0:
                with open(OUT_FILE, "w") as f:
                    json.dump(results, f, indent=4)
                print(f"💾 Progress saved at ID {uid}...")

        except Exception as e:
            print(f"⚠️ Error at {uid}: {e}. Waiting 2s...")
            time.sleep(2)
            continue

    # Final Save
    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"🏁 Finished! Total Users: {len(results)}")

if __name__ == "__main__":
    main()
