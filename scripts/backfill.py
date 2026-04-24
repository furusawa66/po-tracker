#!/usr/bin/env python3
"""
過去PO記事の一括バックフィル

pokabu.net のカテゴリページから過去記事URLを収集し、
csv_ レコードに article_url / announce_date / 各種データを補完。
その後 Yahoo Finance から株価を取得して騰落率を計算する。

GitHub Actions の workflow_dispatch で1回実行する想定。
"""

import requests
from bs4 import BeautifulSoup
import json, re, time
from datetime import datetime, date, timedelta

DATA_FILE = "data/po_records.json"
BASE_URL  = "https://pokabu.net"
HEADERS   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}


def parse_jp_date(text: str, year: int = None) -> str | None:
    if not year:
        year = date.today().year
    m = re.search(r'(\d{1,2})月(\d{1,2})日', text)
    if not m:
        return None
    try:
        mo, dy = int(m.group(1)), int(m.group(2))
        return date(year, mo, dy).isoformat()
    except Exception:
        return None


NON_PO_SLUG_PATTERNS = ["-kansoku", "-yotei", "-kabuka"]


def is_non_po_url(url: str) -> bool:
    return any(p in url for p in NON_PO_SLUG_PATTERNS)


def collect_article_urls() -> list[dict]:
    """pokabu.net/category/po/ を全ページ巡回して記事URLとコードを収集"""
    articles = []
    page = 1
    while True:
        url = f"{BASE_URL}/category/po/page/{page}/" if page > 1 else f"{BASE_URL}/category/po/"
        print(f"  カテゴリページ {page}: {url}")
        try:
            res = requests.get(url, headers=HEADERS, timeout=20)
            if res.status_code != 200:
                print(f"    HTTP {res.status_code} → 終了")
                break
            res.encoding = "utf-8"
            soup = BeautifulSoup(res.text, "html.parser")
        except Exception as e:
            print(f"    エラー: {e}")
            break

        found = 0
        for a in soup.find_all("a", href=re.compile(r'/po/[^/]+/?$')):
            href = a["href"]
            if not href.startswith("http"):
                href = BASE_URL + href
            title = a.get_text(strip=True)
            code_m = re.search(r'[（(](\d{4})[）)]', title)
            if code_m and not is_non_po_url(href):
                articles.append({"url": href, "title": title, "code": code_m.group(1)})
                found += 1

        if found == 0:
            print(f"    記事なし → 終了")
            break

        print(f"    {found} 件取得")
        page += 1
        time.sleep(5)

    # 重複除去
    seen = set()
    unique = []
    for a in articles:
        if a["url"] not in seen:
            seen.add(a["url"])
            unique.append(a)
    print(f"\n合計: {len(unique)} 記事URL収集\n")
    return unique


def scrape_article_data(url: str, code: str = "") -> dict:
    """記事ページからPO情報を抽出"""
    info = {}
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        if res.status_code != 200:
            return info
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
        full_text = soup.get_text(" ", strip=True)
    except Exception as e:
        print(f"    記事取得エラー ({url}): {e}")
        return info

    # 記事公開日（meta）
    pub_meta = soup.find("meta", {"property": "article:published_time"}) or soup.find("meta", {"name": "pubdate"})
    if pub_meta and pub_meta.get("content"):
        info["article_published"] = pub_meta["content"][:10]

    # 年の推定
    year_m = re.search(r'20\d{2}', url)
    art_year = int(year_m.group()) if year_m else date.today().year

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = cells[0].get_text(strip=True)
            val = cells[1].get_text(strip=True)

            if "発表日" in key or "公表日" in key:
                d = parse_jp_date(val, art_year)
                if d:
                    info["announce_date"] = d

            elif "時価総額" in key:
                m = re.search(r'([\d,]+)億', val)
                if m:
                    info["market_cap"] = int(m.group(1).replace(",", ""))

            elif "条件決定日" in key:
                d = parse_jp_date(val, art_year)
                if d:
                    info["decision_date"] = d

            elif "価格決定日" in key or ("発行" in key and "決定日" in key):
                if not info.get("decision_date"):
                    d = parse_jp_date(val, art_year)
                    if d:
                        info["decision_date"] = d

            elif "受渡日" in key and "予定" not in key and "始値" not in key:
                d = parse_jp_date(val, art_year)
                if d:
                    info["delivery_date"] = d

            elif "受渡予定日" in key:
                d = parse_jp_date(val, art_year)
                if d:
                    info["delivery_estimated"] = d

            elif ("発行" in key or "処分" in key or "売出" in key) and "価格" in key and "決定日" not in key:
                m = re.search(r'([\d,]+)円', val)
                if m:
                    info["issue_price"] = int(m.group(1).replace(",", ""))
                dm = re.search(r'([\d.]+)%', val)
                if dm:
                    info["discount_rate"] = float(dm.group(1))

            elif "仮条件" in key:
                info["discount_range"] = val

            elif "希薄化" in key:
                dm = re.search(r'([\d.]+)%', val)
                if dm:
                    info["dilution"] = float(dm.group(1))

            elif "信用" in key and "貸借" in key:
                ct = val.strip()
                if "貸借" in ct:
                    info["lending_type"] = "貸借"
                elif "信用" in ct:
                    info["lending_type"] = "信用"

    # PO規模
    scale_m = re.search(r'(?:最大|合計)?(\d+(?:,\d+)*(?:\.\d+)?)億円規模', full_text)
    if scale_m:
        info["po_scale"] = float(scale_m.group(1).replace(",", ""))

    # 記事本文から確定価格・割引率をフォールバック取得
    if not info.get("issue_price"):
        pm = re.search(r'(?:発行|処分)価格は([\d,]+)円に決定', full_text)
        if pm:
            info["issue_price"] = int(pm.group(1).replace(",", ""))
    if not info.get("discount_rate"):
        dm = re.search(r'割引率は([\d.]+)[％%]', full_text)
        if dm:
            info["discount_rate"] = float(dm.group(1))

    # 主幹事
    lead_managers = []
    co_managers = []
    for table in soup.find_all("table"):
        header_text = table.get_text(" ", strip=True)
        if "主幹事" not in header_text and "証券会社" not in header_text:
            continue
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            role = cells[0].get_text(strip=True)
            name = cells[1].get_text(strip=True)
            if not name or name in ("証券会社名", ""):
                continue
            if "主幹事" in role:
                lead_managers.append(name)
            elif any(k in role for k in ["引受", "委託", "副幹事", "幹事"]):
                co_managers.append(name)
    if lead_managers:
        info["lead_managers"] = lead_managers
    if co_managers:
        info["co_managers"] = co_managers

    return info


def fetch_intraday_15m(code: str, days: int = 60) -> dict:
    """Yahoo Finance から 15 分足を取得。戻り値: {'YYYY-MM-DD': {'HH:MM': {'open','close'}}}"""
    ticker = f"{code}.T"
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=15m&range={days}d"
    bars_by_date = {}
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code != 200:
            return bars_by_date
        data = res.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return bars_by_date
        r = result[0]
        tss = r.get("timestamp", [])
        q = r.get("indicators", {}).get("quote", [{}])[0]
        gmt_offset = r.get("meta", {}).get("gmtoffset", 32400)  # JST デフォルト
        for i, ts in enumerate(tss):
            if not ts or q["close"][i] is None:
                continue
            local_dt = datetime.utcfromtimestamp(ts + gmt_offset)
            dk = local_dt.date().isoformat()
            tk = local_dt.strftime("%H:%M")
            bars_by_date.setdefault(dk, {})[tk] = {
                "open": round(q["open"][i], 2) if q["open"][i] else None,
                "close": round(q["close"][i], 2) if q["close"][i] else None,
            }
    except Exception as e:
        print(f"  15m取得エラー ({code}): {e}")
    return bars_by_date


def fetch_prices(code: str, days: int = 90) -> dict:
    """Yahoo Finance から株価を取得（v8 API → v7 CSV フォールバック）"""
    ticker = f"{code}.T"
    prices = {}

    # まず v8 API を試す
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range={days}d"
    try:
        res = requests.get(url, headers=HEADERS, timeout=12)
        if res.status_code == 200:
            data = res.json()
            result = data.get("chart", {}).get("result")
            if result:
                r = result[0]
                q = r["indicators"]["quote"][0]
                tss = r.get("timestamp", [])
                for i, ts in enumerate(tss):
                    if not ts:
                        continue
                    d = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
                    prices[d] = {
                        "open":  round(q["open"][i],  2) if q["open"][i]  else None,
                        "close": round(q["close"][i], 2) if q["close"][i] else None,
                        "high":  round(q["high"][i],  2) if q["high"][i]  else None,
                    }
                if prices:
                    return prices
    except Exception:
        pass

    # v7 CSV ダウンロード（過去データ対応）
    from_ts = int((datetime.now() - timedelta(days=days)).timestamp())
    to_ts = int(datetime.now().timestamp())
    csv_url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}?period1={from_ts}&period2={to_ts}&interval=1d&events=history"
    try:
        res = requests.get(csv_url, headers=HEADERS, timeout=15)
        if res.status_code == 200 and "Date" in res.text[:50]:
            for line in res.text.strip().split("\n")[1:]:
                parts = line.split(",")
                if len(parts) >= 5:
                    d = parts[0]
                    try:
                        prices[d] = {
                            "open":  round(float(parts[1]), 2) if parts[1] != "null" else None,
                            "close": round(float(parts[4]), 2) if parts[4] != "null" else None,
                            "high":  round(float(parts[2]), 2) if parts[2] != "null" else None,
                        }
                    except ValueError:
                        continue
    except Exception as e:
        print(f"    Yahoo CSV エラー ({code}): {e}")

    return prices


def next_biz_day(d: date) -> date:
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:
        nd += timedelta(days=1)
    return nd


def prev_biz_day(d: date) -> date:
    pd = d - timedelta(days=1)
    while pd.weekday() >= 5:
        pd -= timedelta(days=1)
    return pd


def fill_intraday(rec: dict, bars_by_date: dict):
    """発表日翌日の15分足データから騰落率を計算"""
    ann_str = rec.get("announce_date")
    if not ann_str or not rec.get("next_open"):
        return
    try:
        ann_date = datetime.fromisoformat(ann_str).date()
    except Exception:
        return
    # 翌営業日（祝日含めて最大5日探索）
    nd = ann_date
    for _ in range(7):
        nd = next_biz_day(nd)
        if nd.isoformat() in bars_by_date:
            break
    else:
        return
    bars = bars_by_date.get(nd.isoformat(), {})
    next_open = rec["next_open"]
    if not next_open:
        return

    # 9:00 バーの close = 9:15 時点
    if bars.get("09:00") and not rec.get("next_day_915_ret"):
        c = bars["09:00"].get("close")
        if c:
            rec["next_day_915_ret"] = round((c - next_open) / next_open * 100, 2)

    # 9:15 バーの close = 9:30 時点
    if bars.get("09:15") and not rec.get("next_day_930_ret"):
        c = bars["09:15"].get("close")
        if c:
            rec["next_day_930_ret"] = round((c - next_open) / next_open * 100, 2)

    # 11:15 バーの close = 11:30 前場引け
    if bars.get("11:15") and not rec.get("next_day_morning_ret"):
        c = bars["11:15"].get("close")
        if c:
            rec["next_day_morning_ret"] = round((c - next_open) / next_open * 100, 2)


def fill_prices(rec: dict, prices: dict):
    """株価データから騰落率等を計算してレコードに書き込む"""
    ann_str = rec.get("announce_date")
    dec_str = rec.get("decision_date")
    del_str = rec.get("delivery_date") or rec.get("delivery_estimated")
    if not ann_str:
        return

    try:
        ann_date = datetime.fromisoformat(ann_str).date()
    except Exception:
        return

    next_day = next_biz_day(ann_date).isoformat()

    # 翌日始値
    if not rec.get("next_open") and next_day in prices:
        p = prices[next_day]
        if p["open"]:
            rec["next_open"] = p["open"]

    # 発表日終値（翌日GU判定用）
    if not rec.get("announce_day_close") and ann_str in prices:
        p = prices[ann_str]
        if p.get("close"):
            rec["announce_day_close"] = p["close"]

    # 翌日 GU/GD ギャップ率（翌日始値 vs 発表日終値）
    if rec.get("next_open") and rec.get("announce_day_close") and not rec.get("next_day_gu_pct"):
        rec["next_day_gu_pct"] = round((rec["next_open"] - rec["announce_day_close"]) / rec["announce_day_close"] * 100, 2)

    # 決定日始値・終値
    if dec_str and dec_str in prices and not rec.get("dec_open"):
        p = prices[dec_str]
        if p["open"] and p["close"]:
            rec["dec_open"] = p["open"]
            rec["dec_close"] = p["close"]

    # 騰落率
    if rec.get("dec_open") and rec.get("next_open") and not rec.get("ret_open"):
        rec["ret_open"] = round((rec["dec_open"] - rec["next_open"]) / rec["next_open"] * 100, 2)
        rec["ret_close"] = round((rec["dec_close"] - rec["next_open"]) / rec["next_open"] * 100, 2)

    # 受渡日
    if del_str and del_str in prices and not rec.get("delivery_open"):
        p = prices[del_str]
        if p["open"] and p["close"]:
            rec["delivery_open"] = p["open"]
            rec["delivery_close"] = p["close"]
            rec["delivery_ret"] = round((p["close"] - p["open"]) / p["open"] * 100, 2)

    # 発表日翌日の日中データ（15分足）
    if next_day in prices:  # 翌日の日足データがある場合のみ
        pass  # intraday は別途 fill_intraday で処理

    # 受渡日前日終値 & GU率（ギャップ率）
    if del_str and not rec.get("prev_close_before_delivery"):
        try:
            del_date = datetime.fromisoformat(del_str).date()
            # 前営業日から遡って、prices にある日を探す（祝日対応）
            for _ in range(7):
                del_date = prev_biz_day(del_date)
                if del_date.isoformat() in prices:
                    p = prices[del_date.isoformat()]
                    if p["close"]:
                        rec["prev_close_before_delivery"] = p["close"]
                        if rec.get("delivery_open"):
                            rec["delivery_gap_pct"] = round((rec["delivery_open"] - p["close"]) / p["close"] * 100, 2)
                    break
        except Exception:
            pass


def main():
    print(f"\n{'='*50}")
    print(f"PO バックフィル開始: {date.today()}")
    print(f"{'='*50}\n")

    # データ読み込み
    with open(DATA_FILE, encoding="utf-8") as f:
        data = json.load(f)
    records = data.get("records", data)

    # 既存レコードで補完が必要なもの
    need_update = {r["code"]: r for r in records
                   if r.get("code") and r.get("article_url")
                   and (not r.get("announce_date") or r.get("discount_rate") is None)}
    print(f"補完対象: {len(need_update)} 件\n")

    # 既存の (code, year) ペアを収集
    existing_keys = set()
    for r in records:
        if r.get("code") and r.get("year"):
            existing_keys.add((r["code"], r["year"]))

    # ① 記事URL収集
    print("[1] pokabu.net カテゴリページから記事URL収集...")
    articles = collect_article_urls()

    # ② 記事スクレイピング（既存補完 + 新規追加）
    print("[2] 記事スクレイピング...")
    matched = 0
    added = 0
    for art in articles:
        code = art["code"]
        year_m = re.search(r'20(\d{2})', art["url"])
        art_year = (2000 + int(year_m.group(1))) if year_m else date.today().year

        # 既存レコードの補完
        if code in need_update:
            rec = need_update[code]
            if rec.get("year") and rec["year"] != art_year:
                pass  # 年が違う → 新規追加チェックへ
            else:
                print(f"  補完: {rec.get('name')} ({code}): {art['url']}")
                info = scrape_article_data(art["url"], code)
                time.sleep(5)
                if info:
                    rec["article_url"] = art["url"]
                    for field in ["announce_date", "decision_date", "delivery_date", "delivery_estimated",
                                  "market_cap", "po_scale", "discount_range",
                                  "dilution", "lending_type", "lead_managers", "co_managers"]:
                        if info.get(field) and not rec.get(field):
                            rec[field] = info[field]
                    if info.get("issue_price"):
                        rec["issue_price"] = info["issue_price"]
                    if info.get("discount_rate"):
                        rec["discount_rate"] = info["discount_rate"]
                    if not rec.get("announce_date") and info.get("article_published"):
                        rec["announce_date"] = info["article_published"]
                    if rec.get("announce_date"):
                        rec["announce_date_confirmed"] = True
                        rec["id"] = f"{code}_{rec['announce_date'].replace('-', '')}"
                    if rec.get("decision_date"):
                        rec["decision_date_confirmed"] = True
                    if rec.get("po_scale") and rec.get("market_cap"):
                        rec["po_pct"] = round(rec["po_scale"] / rec["market_cap"] * 100, 1)
                    lt = rec.get("lending_type", "")
                    rec["alert"] = "" if lt == "貸借" else ("注意" if lt == "信用" else rec.get("alert", ""))
                    matched += 1
                    del need_update[code]
                continue

        # 新規追加: データに存在しない記事 → 新レコード作成
        if (code, art_year) in existing_keys:
            continue

        print(f"  新規: {art['title']} ({code}): {art['url']}")
        info = scrape_article_data(art["url"], code)
        time.sleep(5)
        if not info:
            continue

        announce = info.get("announce_date") or info.get("article_published") or ""
        name_m = re.search(r'[】](.*?)[（(]', art["title"])
        name = name_m.group(1).strip() if name_m else art["title"]
        lt = info.get("lending_type", "")
        is_reit = any(k in name for k in ["リート", "投資法人"]) or any(k in (art["title"] or "") for k in ["リート", "投資法人"])

        new_rec = {
            "id": f"{code}_{announce.replace('-','')}" if announce else f"{code}_{art_year}",
            "code": code, "name": name,
            "type": "リート" if is_reit else "普通",
            "alert": "" if lt == "貸借" else ("注意" if lt == "信用" else ""),
            "lending_type": lt,
            "announce_date": announce, "announce_date_confirmed": bool(announce),
            "year": art_year,
            "decision_date": info.get("decision_date"),
            "decision_date_confirmed": bool(info.get("decision_date")),
            "delivery_date": info.get("delivery_date"),
            "delivery_estimated": info.get("delivery_estimated"),
            "market_cap": info.get("market_cap"),
            "po_scale": info.get("po_scale"),
            "po_pct": None,
            "new_shares": None, "sold_shares": None, "oa_shares": None,
            "shares_outstanding": None,
            "dilution": info.get("dilution"),
            "issue_price": info.get("issue_price"),
            "discount_rate": info.get("discount_rate"),
            "discount_range": info.get("discount_range"),
            "lead_managers": info.get("lead_managers", []),
            "co_managers": info.get("co_managers", []),
            "article_url": art["url"],
            "next_open": None, "max_price": None, "open_to_max": None,
            "dec_open": None, "dec_close": None,
            "ret_open": None, "ret_close": None,
            "delivery_open": None, "delivery_close": None, "delivery_ret": None,
            "treasury_shares": None,
            "memo": "", "status": "pending",
        }
        if new_rec["po_scale"] and new_rec["market_cap"]:
            new_rec["po_pct"] = round(new_rec["po_scale"] / new_rec["market_cap"] * 100, 1)

        records.append(new_rec)
        existing_keys.add((code, art_year))
        added += 1

    print(f"\n補完: {matched} 件 / 新規追加: {added} 件\n")

    # ③ 株価取得（next_open / delivery_open / prev_close_before_delivery が未取得のレコード）
    need_prices = [r for r in records if r.get("announce_date") and r.get("announce_date_confirmed")
                   and (not r.get("next_open") or not r.get("delivery_open") or not r.get("prev_close_before_delivery"))]
    print(f"[3] 株価取得: {len(need_prices)} 件...")
    for rec in need_prices:
        code = rec.get("code")
        if not code:
            continue
        # 発表日から今日までの日数 + 余裕
        try:
            ann = datetime.fromisoformat(rec["announce_date"]).date()
            days = (date.today() - ann).days + 30
            days = max(days, 120)
        except Exception:
            days = 120
        print(f"  {rec.get('name')} ({code}) range={days}d")
        prices = fetch_prices(code, days=days)
        if prices:
            fill_prices(rec, prices)
        time.sleep(0.5)

    # ③-2 Intraday 15m 取得（発表日が60日以内のレコードのみ）
    cutoff = date.today() - timedelta(days=60)
    need_intraday = [r for r in records if r.get("announce_date") and r.get("next_open")
                     and not r.get("next_day_morning_ret")]
    intraday_targets = []
    for r in need_intraday:
        try:
            ad = datetime.fromisoformat(r["announce_date"]).date()
            if ad >= cutoff:
                intraday_targets.append(r)
        except Exception:
            continue
    print(f"[4] Intraday 15m 取得: {len(intraday_targets)} 件...")
    # コード単位でまとめて取得（同じコードが複数レコードあればキャッシュ効く）
    intraday_cache = {}
    for rec in intraday_targets:
        code = rec.get("code")
        if not code:
            continue
        if code not in intraday_cache:
            print(f"  {rec.get('name')} ({code}) 15m")
            intraday_cache[code] = fetch_intraday_15m(code, days=60)
            time.sleep(0.5)
        bars = intraday_cache[code]
        if bars:
            fill_intraday(rec, bars)

    # ④ 保存
    out = {"records": records, "last_updated": datetime.now().isoformat(), "count": len(records)}
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    filled = sum(1 for r in records if r.get("announce_date") and r.get("next_open"))
    print(f"\n完了: announce_date+next_open 取得済み {filled} / {len(records)} 件")


if __name__ == "__main__":
    main()
