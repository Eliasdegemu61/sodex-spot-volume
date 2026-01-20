import requests
import json
import time
import sys
from decimal import Decimal, getcontext

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
END_ID = 8000               # High limit with circuit breaker
CHUNK_SIZE = 50             # Save more often
ZERO_FEE_STOP_LIMIT = 100   # The "Team Address" trick
ADDRESS_URL = "https://sodex.dev/mainnet/chain/user/{}/address"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_market_prices():
    try:
        syms = requests.get("https://mainnet-gw.sodex.dev/bolt/symbols?names").json().get('data', [])
        id_map = {str(i['symbolID']): i['name'] for i in syms}
        prices_data = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        price_map = {p['s']: Decimal(str(p['p'])) for p in prices_data}
        return {s_id: price_map.get(name, Decimal('0')) for s_id, name in id_map.items()}
    except Exception as e:
        print(f"❌ Price Fetch Error: {e}", flush=True)
        return {}

def main():
    # We call it 'active_prices' here to be clear
    active_prices = get_market_prices()
    results = {}
    consecutive_zeros = 0
    
    print(f"🚀 Starting RELIABLE Scan | IDs {START_ID} to {END_ID}", flush=True)

    for uid in range(START_ID, END_ID + 1):
        try:
            # 1. Check if ID exists
            resp = requests.get(ADDRESS_URL.format(uid), timeout=10).json()
            if resp.get("code") != 0:
                continue 
            
            addr = resp["data"]["address"]
            vol, fees, trades_found = Decimal('0'), Decimal('0'), 0
            offset, limit = 0, 100
            
            # 2. Process ALL trades
            while True:
                r = requests.get(f"{TRADE_URL}?account_id={uid}&limit={limit}&offset={offset}", timeout=15).json()
                trades = r.get('data', [])
                if not trades: break
                
                trades_found += len(trades)
                for t in trades:
                    # FIXED: Using 'active_prices' consistently
                    p = active_prices.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                    vol += (Decimal(str(t['quantity'])) * p)
                    fees += (Decimal(str(t['fee'])) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t['fee']))
                
                offset += limit
                if len(trades) < limit: break
            
            # 3. Logic for findings
            if trades_found > 0:
                # Track zero fee streak for the "Team Address" trick
                if fees == 0:
                    consecutive_zeros += 1
                else:
                    consecutive_zeros = 0
                
                results[addr] = {
                    "id": uid, "vol": float(round(vol, 2)), 
                    "fee": float(round(fees, 4)), "ts": int(time.time())
                }
                # Real-time console output
                print(f"✅ User {uid} | Vol: ${round(vol, 2)} | Fees: ${round(fees, 4)}", flush=True)
            
            # --- CIRCUIT BREAKER ---
            if consecutive_zeros >= ZERO_FEE_STOP_LIMIT:
                print(f"🛑 Stopping at ID {uid}: Hit {ZERO_FEE_STOP_LIMIT} zero-fee users.", flush=True)
                break

            # Save progress periodically
            if uid % CHUNK_SIZE == 0:
                with open(OUT_FILE, "w") as f:
                    json.dump(results, f, indent=4)

        except Exception as e:
            print(f"⚠️ System Error at {uid}: {e}", flush=True)
            time.sleep(1)
            continue

    # Final Save
    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)
    print(f"🏁 Finished! Total Active Users: {len(results)}", flush=True)

if __name__ == "__main__":
    main()
