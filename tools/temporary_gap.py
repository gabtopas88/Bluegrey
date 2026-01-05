from polygon import RESTClient
import src.config as config
from datetime import date

client = RESTClient(config.POLYGON_API_KEY)

print("🕵️ PROBING THE DARK ZONE: 2020-10-18 to 2020-11-15")
aggs = []
for a in client.list_aggs(
    ticker="C:EURUSD",
    multiplier=1,
    timespan="minute",
    from_="2020-10-24",
    to="2020-11-14",
    limit=50000
):
    aggs.append(a)

print(f"🔎 Result: Found {len(aggs)} bars.")