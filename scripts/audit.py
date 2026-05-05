#!/usr/bin/env python3
"""データ品質監査: 各レコードに対して分析適合性のフラグを計算し
data/po_audit.json に書き出す。

フラグは po_records.json には書き戻さず別ファイルに出力するため、
主データの diff churn を避ける。フロントは両ファイルを読んで
データを統合する。

呼び出し:
    python scripts/audit.py            # サマリ表示 + データ書き出し
    python scripts/audit.py --quiet    # サマリ非表示 (CI用途)
"""

import json, os, sys
from collections import Counter
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from utils import atomic_write_json

DATA_FILE = "data/po_records.json"
AUDIT_FILE = "data/po_audit.json"

# split adjustment 検出の許容範囲 (issue_price / delivery_open)
# PO割引率は通常 0-7% なので比率は 0.93-1.00 付近に集中するはず。
# 0.75 を下回る or 1.35 を超える場合、株式分割等の調整値混入を疑う。
SPLIT_RATIO_MIN = 0.75
SPLIT_RATIO_MAX = 1.35


def compute_flags(rec: dict, code_counts: dict[str, int]) -> list[str]:
    flags: list[str] = []

    # 受渡価格欠損 (status=complete でも実価格が無いケースを検出)
    if not (rec.get("delivery_open") and rec.get("delivery_close")):
        flags.append("missing_delivery_prices")

    # announce_date 欠損
    if not rec.get("announce_date"):
        flags.append("missing_announce_date")

    # legacy フラグ (旧CSVインポート由来など低信頼)
    if rec.get("legacy"):
        flags.append("legacy_record")

    # 同一コードに複数レコード存在 (年またぎ別イベント等。informational)
    if code_counts.get(rec.get("code"), 0) > 1:
        flags.append("repeated_code")

    # 株式分割等による issue_price と delivery_open の基準ずれ
    ip = rec.get("issue_price")
    do = rec.get("delivery_open")
    if ip and do and do > 0:
        ratio = ip / do
        if ratio < SPLIT_RATIO_MIN or ratio > SPLIT_RATIO_MAX:
            flags.append("issue_price_delivery_ratio_outlier")
            flags.append("possible_split_adjustment_mismatch")
            # discount_rate は dec_close と issue_price の比なので同様に信用できない
            if rec.get("discount_rate") is not None:
                flags.append("discount_rate_untrusted")

    # 期待値分析に使えるか
    ev_blockers = ("missing_delivery_prices", "missing_announce_date",
                   "legacy_record", "possible_split_adjustment_mismatch")
    required_fields = ("announce_date", "next_open", "delivery_open", "delivery_close")
    has_required = all(rec.get(f) for f in required_fields)
    if not has_required or any(b in flags for b in ev_blockers):
        flags.append("incomplete_for_ev_analysis")

    return flags


def main(quiet: bool = False):
    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)
    records = data["records"]
    n = len(records)

    code_counts = Counter(r.get("code") for r in records if r.get("code"))

    flags_by_id: dict[str, list[str]] = {}
    summary_counter: Counter = Counter()
    for r in records:
        rid = r.get("id")
        if not rid:
            continue
        f_list = compute_flags(r, code_counts)
        if f_list:
            flags_by_id[rid] = f_list
            for f in f_list:
                summary_counter[f] += 1

    analysis_ready = sum(1 for r in records
                        if flags_by_id.get(r.get("id"), []).count("incomplete_for_ev_analysis") == 0)

    audit = {
        "generated_at": datetime.now().isoformat(),
        "source_file": DATA_FILE,
        "source_count": n,
        "rules": {
            "split_ratio_min": SPLIT_RATIO_MIN,
            "split_ratio_max": SPLIT_RATIO_MAX,
            "required_for_ev_analysis": ["announce_date", "next_open",
                                          "delivery_open", "delivery_close",
                                          "not legacy_record",
                                          "not possible_split_adjustment_mismatch"],
        },
        "summary": {
            "total_records": n,
            "analysis_ready": analysis_ready,
            "flag_counts": dict(summary_counter),
        },
        "flags_by_id": flags_by_id,
    }

    atomic_write_json(AUDIT_FILE, audit)

    if not quiet:
        print(f"\n{'='*50}")
        print(f"データ品質監査: {datetime.now().isoformat()}")
        print(f"{'='*50}")
        print(f"総レコード:           {n}")
        print(f"分析適合 (ev_ready):  {analysis_ready}")
        print(f"フラグ付きレコード:   {len(flags_by_id)}\n")
        print("フラグ別件数:")
        for f, c in summary_counter.most_common():
            print(f"  {f:42s} {c:>4}")
        print(f"\n書き出し: {AUDIT_FILE}")


if __name__ == "__main__":
    main(quiet="--quiet" in sys.argv)
