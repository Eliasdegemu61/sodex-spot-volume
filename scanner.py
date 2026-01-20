import requests
import json
import time
from decimal import Decimal, getcontext
from concurrent.futures import ThreadPoolExecutor

getcontext().prec = 50

# --- CONFIG ---
START_ID = 1000
END_ID = 5000               # The "Upper Limit" we found
NUM_CHUNKS = 10             # Divide the work into 10 threads
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

def scan_range(start, end, price_map):
    """Function given to each thread to scan its specific 10% chunk."""
    chunk_results = {}
    print(f"🧵 Thread started for range: {start} - {end}")
    
    for curr_id in range(start, end + 1):
        try:
            resp = requests.get(ADDRESS_URL.format(curr_id), timeout=10).json()
            if resp.get("code") != 0: continue 
            
            addr = resp["data"]["address"]
            vol, fees = Decimal('0'), Decimal('0')
            off, lim = 0, 100
            user_trades = 0
            
            while True:
                r = requests.get(f"{TRADE_URL}?account_id={curr_id}&limit={lim}&offset={off}", timeout=10).json()
                trades = r.get('data', [])
                if not trades: break
                
                user_trades += len(trades)
                for t in trades:
                    p = price_map.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                    vol += (Decimal(str(t['quantity'])) * p)
                    fees += (Decimal(str(t['fee'])) * p) if int(t.get('side', 1)) == 1 else Decimal(str(t['fee']))
                
                off += lim
                if len(trades) < lim: break
            
            if user_trades > 0:
                chunk_results[addr] = {
                    "id": curr_id, "vol": float(round(vol, 2)), 
                    "fee": float(round(fees, 4)), "ts": int(time.time())
                }
                print(f"✅ Chunk Found: {curr_id} (${round(vol, 2)})", flush=True)

        except: continue
    return chunk_results

def main():
    prices = get_market_prices()
    all_results = {}
    
    # 1. Calculate the chunks
    total_range = END_ID - START_ID
    chunk_size = total_range // NUM_CHUNKS
    
    ranges = []
    for i in range(NUM_CHUNKS):
        s = START_ID + (i * chunk_size)
        # Ensure the last chunk goes all the way to END_ID
        e = (START_ID + (i + 1) * chunk_size - 1) if i < NUM_CHUNKS - 1 else END_ID
        ranges.append((s, e))

    # 2. Launch Threads
    print(f"🚀 Launching {NUM_CHUNKS} threads to scan 10% of the IDs each...")
    with ThreadPoolExecutor(max_workers=NUM_CHUNKS) as executor:
        future_to_range = {executor.submit(scan_range, r[0], r[1], prices): r for r in ranges}
        
        for future in future_to_range:
            chunk_data = future.result()
            all_results.update(chunk_data) # Merge the 10% into the final list

    # 3. Save Final JSON
    with open(OUT_FILE, "w") as f:
        json.dump(all_results, f, indent=4)
    print(f"💾 Success! Combined all chunks. Total users: {len(all_results)}")

if __name__ == "__main__":
    main()
