# Dataset facts

For the Dataset Analysis section (the rubric's "strong focus on dataset analysis" and "handling of
long-tail distributions"). Numbers only. Sources are in the last column; "fact sheet" means the
dataset fact sheet in `report.tex` (PR #7), checked against the data on 24 Nov.

## Size and shape

| Fact | Value | Source |
|---|---|---|
| Period | Mon 8 to Sun 14 Nov 2021 | course brief, Zenodo 6382482 |
| Events, whole week, every type | 289,116,808 | fact sheet |
| Events, Mon-Fri, every type | 289,057,297 (so the weekend has 59,511) | `results/summary.md`, all-events run |
| Symbols, whole week | 5,502 (course brief says 5,504) | fact sheet |
| Events by exchange, week | FR 158,331,078 (54.8%), NL 79,991,190 (27.7%), ETR 50,794,540 (17.6%) | fact sheet; brief says 55 / 28 / 17% |
| Symbols by exchange | FR 2,540, NL 750, ETR 2,212 | fact sheet |
| Trades (Last > 0), Mon-Fri | 59,391,670, about 20.5% of the weekday events | `producer.json`, trades run |
| Trades by exchange, Mon-Fri | ETR 31,996,003 (53.9%), FR 21,367,417 (36.0%), NL 6,028,250 (10.2%) | per-exchange runs |
| Symbols with at least one trade, Mon-Fri | 4,970 (FR 2,285, NL 559, ETR 2,126) | validation.json per run |
| 5-minute windows with a trade, Mon-Fri | 1,855,645 (FR 1,043,637, NL 255,249, ETR 556,759) | validation.json per run |
| Advisories, Mon-Fri | 11,349 (FR 6,049, NL 1,409, ETR 3,891); 6,379 excluding first windows | validation.json per run |
| Monday trades by security type | 9.81 M of 11.79 M (83%) on indices (I), 1.98 M on equities (E) | fact sheet |
| Weekend | events but almost no trades, so experiments use Mon-Fri | fact sheet, README |
| Columns used | ID, SecType, Date, Time, Last, Trading time (6 of 39) | `TickParser.java`, `producer.py` |

Note the flip: by events Paris is biggest and Frankfurt smallest; by trades Frankfurt is biggest
and Amsterdam smallest. Course brief: equities 239.5 M events vs indices 49.5 M, but trading
activity is higher on indices (49 M) than equities (10 M).

## Long tail

| Fact | Value | Source |
|---|---|---|
| Top 1% of symbols, share of all events | 47.3% | fact sheet (`analysis/longtail.py`) |
| Top 10% of symbols, share of all events | 84.2% | same |
| Symbols that make up half of all events | 63 | same |
| Busiest symbol by events | RDSA.NL, 6.9 M events | same |
| Median events per symbol | 11,225 | same |
| Trades: top 10% of symbols | about half of all trades (much flatter than events) | same, right panel of the figure |
| Busiest symbols by trades | flat at about 160k trades over the week for the top ~150 | same, left panel |
| Monday: busiest symbol by trades | A0X7J3.ETR, 32,371 trades; median symbol 2,102 | fact sheet |
| Windows per symbol over Mon-Fri | median 530; 1,605 of 4,970 symbols have fewer than 100, 1,263 fewer than 38 | fact sheet |

Figure: `submission/report/figures/longtail-week.png` (already in the report draft).

What the long tail does inside the job (from the experiments, see `results.md`): window-operator
load max/mean 1.10 at 4 cores and 1.15 at 8. Kafka partition sizes are less even (max/mean 1.26 over
8 partitions).

## Data quality and the two clocks

| Fact | Value | Source |
|---|---|---|
| `Time` (receive time) precision | whole seconds: 99.99% of Monday's trades end in `.000` | `analysis/clocks.py` (fact sheet) |
| Trades with `Time` more than 60 s from `Trading time`, Monday | 861 | same |
| Trades older than one already seen for the same symbol, Monday | 0 | same |
| Late trades on the real Monday at any bound 0-120 s | 0 | `results/ooo-sweep/20261117-230210` |
| Placeholder trading time `01:00:00.000` | appears on some days, not Monday; the job drops it | `analysis/late_check.py`, `TickParser` |
| Trades dropped by the router in the weekday runs | 5,506 (FR 4,371, NL 1,135, ETR 0): placeholder-time or malformed | `facts/derived-facts.md` |
| Event types | not marked; a trade is an event with a non-empty, positive `Last` | course brief 2.2, `TickParser` |
| Order across exchanges | total order per symbol across exchanges not guaranteed (brief); symbols are per exchange anyway (`ID.[exchange]`) | course brief 2.1 |

If you want exact per-day placeholder counts in the text, re-run `analysis/late_check.py` on the Mac
(it needs the parquet files in `data/`, which aren't in the repo).

## Things the data forced on the design (pointers into design-decisions.md)

- Trades are a fifth of the events, so filtering at the source matters (D7).
- `Time` is too coarse and sometimes far off, so event time comes from `Trading time` (D2).
- Placeholder time `01:00:00.000` (D8).
- Many symbols trade rarely: EMAs warm up slowly from 0, first-window buys (D12, D13, D23).
- Skewed symbol sizes vs hash partitioning (D3, D11).
