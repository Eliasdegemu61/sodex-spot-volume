import requests
import json
import time

# --- CONFIGURATION ---
BASE_ID = 1000
ADDRESS_URL = "https://sodex.dev/mainnet/chain/user/{}/address"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_market_prices():
    """Maps Symbol ID -> Current Mark Price."""
    try:
        syms = requests.get("https://mainnet-gw.sodex.dev/bolt/symbols?names").json().get('data', [])
        id_map = {str(i['symbolID']): i['name'] for i in syms}
        
        prices = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        price_map = {p['s']: float(p['p']) for p in prices}
        
        return {s_id: price_map.get(name, 0) for s_id, name in id_map.items()}
    except Exception as e:
        print(f"Price Fetch Error: {e}")
        return {}

def get_stats(acc_id, price_map):
    """Calculates Volume and Fees with the 2% fee logic."""
    vol, fees = 0.0, 0.0
    off, lim = 0, 100
    while True:
        try:
            r = requests.get(f"{TRADE_URL}?account_id={acc_id}&limit={lim}&offset={off}").json()
            trades = r.get('data', [])
            if not trades: break
            
            for t in trades:
                # Resolve price (live price first, then execution price)
                p = price_map.get(str(t['symbol_id'])) or float(t.get('price', 0))
                qty = float(t.get('quantity', 0))
                f_raw = float(t.get('fee', 0))
                
                v_usd = qty * p
                vol += v_usd
                
                # Fee Fix logic
                f_usd = f_raw * p
                if f_usd > (v_usd * 0.02):
                    fees += f_raw # Fee was already USD
                else:
                    fees += f_usd # Fee was token amount
            
            off += lim
            if len(trades) < lim: break
        except:
            break
    return round(vol, 2), round(fees, 2)

def main():
    price_map = get_market_prices()
    results = {}
    curr_id = BASE_ID
    
    print(f"🚀 Starting SoDEX Discovery from ID {BASE_ID}...")

    while True:
        try:
            resp = requests.get(ADDRESS_URL.format(curr_id))
            data = resp.json()

            # The 404 trigger to stop the script
            if data.get("code") == 404:
                print(f"🏁 Upper limit found at ID {curr_id}. Stopping.")
                break
            
            if data.get("code") == 0:
                addr = data["data"]["address"]
                v, f = get_stats(curr_id, price_map)
                
                results[addr] = {
                    "id": curr_id,
                    "vol": v,
                    "fee": f,
                    "ts": int(time.time())
                }
                print(f"✅ User {curr_id}: {addr[:8]}... Vol: ${v}")
            
            curr_id += 1
            time.sleep(0.1) # Prevent rate limiting
        except Exception as e:
            print(f"Error at ID {curr_id}: {e}")
            break

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)

if __name__ == "__main__":
    main()
