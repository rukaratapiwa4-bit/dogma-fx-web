"""
Run this on YOUR PC to verify OANDA connection.
Place in the same folder as your system files.

Run:
    python test_connection.py
"""
import requests

API_KEY    = "2d1503350b36f78df033bd4d83d50d02-3b7ef95ac36a308772177787560613cb"
ACCOUNT_ID = "101-003-39651383-001"

headers = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type" : "application/json",
}

print("=" * 50)
print("OANDA CONNECTION TEST")
print("=" * 50)

# Test 1: Account
print("\n[1] Account info...")
url = f"https://api-fxpractice.oanda.com/v3/accounts/{ACCOUNT_ID}"
r   = requests.get(url, headers=headers, timeout=10)
print(f"    Status: {r.status_code}")
if r.status_code == 200:
    data = r.json()
    acc  = data["account"]
    print(f"    ✅ Connected!")
    print(f"    Balance    : {acc['balance']} {acc['currency']}")
    print(f"    Account    : {acc['id']}")
    print(f"    Open trades: {acc['openTradeCount']}")
    print(f"    NAV        : {acc['NAV']}")
else:
    print(f"    ❌ Failed: {r.text[:200]}")

# Test 2: EUR/USD price
print("\n[2] Price feed (EUR/USD)...")
url2 = "https://api-fxpractice.oanda.com/v3/instruments/EUR_USD/candles?count=3&granularity=M1"
r2   = requests.get(url2, headers=headers, timeout=10)
if r2.status_code == 200:
    candles = r2.json().get("candles", [])
    print(f"    ✅ Price feed working!")
    for c in candles:
        print(f"    EUR/USD | C={c['mid']['c']} | {c['time'][:19]}")
else:
    print(f"    ❌ Failed: {r2.text[:200]}")

# Test 3: All pairs live prices
print("\n[3] All pairs live prices...")
pairs = ["EUR_USD","GBP_USD","USD_JPY","AUD_USD","USD_CAD","NZD_USD"]
url3  = f"https://api-fxpractice.oanda.com/v3/accounts/{ACCOUNT_ID}/pricing?instruments={'%2C'.join(pairs)}"
r3    = requests.get(url3, headers=headers, timeout=10)
if r3.status_code == 200:
    print(f"    ✅ Live prices:")
    for p in r3.json().get("prices", []):
        bid = p['bids'][0]['price']
        ask = p['asks'][0]['price']
        print(f"    {p['instrument']:10s} | bid={bid} ask={ask}")
else:
    print(f"    ❌ Failed: {r3.text[:200]}")

print("\n" + "=" * 50)
print("If all ✅ — run: python main.py")
print("=" * 50)
