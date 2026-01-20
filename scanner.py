import requests
import json
import time
from decimal import Decimal, getcontext

# High precision for crypto calculations
getcontext().prec = 50

# --- CONFIGURATION ---
BASE_ID = 1000
ADDRESS_URL = "https://sodex.dev/mainnet/chain/user/{}/address"
TRADE_URL = "https://mainnet-data.sodex.dev/api/v1/spot/trades"
OUT_FILE = "spot_market_stats.json"

def get_market_data():
    """Maps Symbol ID -> Current Decimal Price."""
    try:
        syms = requests.get("https://mainnet-gw.sodex.dev/bolt/symbols?names").json().get('data', [])
        id_map = {str(i['symbolID']): i['name'] for i in syms}
        prices = requests.get("https://mainnet-gw.sodex.dev/futures/fapi/market/v1/public/q/mark-price").json().get('data', [])
        price_map = {p['s']: Decimal(str(p['p'])) for p in prices}
        return {s_id: price_map.get(name, Decimal('0')) for s_id, name in id_map.items()}
    except: return {}

def get_user_stats(acc_id, price_map):
    """Calculates strict volume and fees based on Buy/Sell side."""
    total_vol, total_fees = Decimal('0'), Decimal('0')
    off, lim = 0, 100
    while True:
        try:
            r = requests.get(f"{TRADE_URL}?account_id={acc_id}&limit={lim}&offset={off}").json()
            trades = r.get('data', [])
            if not trades: break
            for t in trades:
                price = price_map.get(str(t['symbol_id'])) or Decimal(str(t.get('price', '0')))
                qty = Decimal(str(t.get('quantity', '0')))
                raw_fee = Decimal(str(t.get('fee', '0')))
                side = int(t.get('side', 1)) 

                # Volume is always qty * price
                total_vol += (qty * price)

                # Fee Logic: Side 1 (Buy) = Token Fee | Side 2 (Sell) = USDC Fee
                if side == 1:
                    total_fees += (raw_fee * price) # Multiply Token to USD
                else:
                    total_fees += raw_fee # Already USDC
            off += lim
            if len(trades) < lim: break
        except: break
    return float(round(total_vol, 2)), float(round(total_fees, 4))

def main():
    prices = get_market_data()
    results = {}
    curr_id = BASE_ID
    print(f"🚀 Scanning from ID {BASE_ID}...")

    while True:
        resp = requests.get(ADDRESS_URL.format(curr_id)).json()
        # 404 means we found the upper limit
        if resp.get("code") == 404:
            print(f"🏁 Limit found at {curr_id}")
            break
        
        if resp.get("code") == 0:
            addr = resp["data"]["address"]
            v, f = get_user_stats(curr_id, prices)
            results[addr] = {
                "id": curr_id,
                "vol_usd": v,
                "fee_usd": f,
                "last_updated": int(time.time())
            }
            print(f"✅ {curr_id} | {addr[:10]}... | Vol: ${v}")
        
        curr_id += 1
        time.sleep(0.1) # Be nice to the API

    with open(OUT_FILE, "w") as f:
        json.dump(results, f, indent=4)

if __name__ == "__main__":
    main()
