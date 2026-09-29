import pandas as pd

DAYS = ["08-11-21", "09-11-21", "10-11-21", "11-11-21", "12-11-21"]
FIVE_MIN = pd.Timedelta(minutes=5)

for day in DAYS:
    p = pd.read_parquet(f"../data/prices-{day}.parquet",
                        columns=["ID", "SecType", "Time", "Trading time", "Last"])
    placeholder = p["Trading time"] == "01:00:00.000"

    # Late = older than a trade already seen for the same symbol, in file order
    tt = pd.to_timedelta(p["Trading time"])
    latest = tt.groupby(p["ID"]).cummax()
    late = tt < latest
    # ...and late enough that its 5-minute window had already been passed
    passed_window = (tt // FIVE_MIN) < (latest // FIVE_MIN)

    print(f"{day}: 01:00:00.000 stamps {placeholder.sum():,} ({p.loc[placeholder, 'ID'].nunique()} symbols) | "
          f"late {late.sum():,} | in a passed window {passed_window.sum():,} | "
          f"passed-window but NOT placeholder {(passed_window & ~placeholder).sum():,} | "
          f"before 07:00 {(p['Trading time'] < '07:00').sum():,}")

    if placeholder.any():
        print("   placeholder exchanges:", p.loc[placeholder, "ID"].str.rsplit(".", n=1).str[-1].value_counts().to_dict(),
              "| received at:", p.loc[placeholder, "Time"].str[:5].value_counts().head(3).to_dict())
    other = passed_window & ~placeholder
    if other.any():
        print(p.loc[other, ["ID", "SecType", "Time", "Trading time", "Last"]].head(5).to_string())