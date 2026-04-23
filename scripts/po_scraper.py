#!/usr/bin/env python3
"""
PO自動トラッカー — pokabu.net 完全対応版

取得データ:
  pokabu.net/schedule  → 価格決定日・受渡日・貸借区分（売り禁フラグ）
  pokabu.net/po/[slug] → PO規模（億）・時価総額・受渡予定日・種類（普通/リート）
  Yahoo Finance JP      → 翌日始値・最高値・決定日始値/終値 → 騰落率自動計算
"""

import requests
from bs4 import BeautifulSoup
import json, os, re, time
from datetime import datetime, date, timedelta

DATA_FILE = "data/po_records.json"
BASE_URL  = "https://pokabu.net"
HEADERS   = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
THIS_YEAR = date.today().year

# ── ユーティリティ ────────────────────────────────────────────────────────────

def next_biz_day(d: date) -> date:
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:
        nd += timedelta(days=1)
    return nd

def prev_biz_days(d: date, n: int) -> date:
    """n営業日前の日付を返す（祝日は考慮しない）"""
    result = d
    count  = 0
    while count < n:
        result -= timedelta(days=1)
        if result.weekday() < 5:
            count += 1
    return result

def parse_jp_date(text: str, year: int = None) -> str | None:
    """'4月6日' '4月6日(月)' → 'YYYY-MM-DD'"""
    if not year:
        year = THIS_YEAR
    m = re.search(r'(\d{1,2})月(\d{1,2})日', text)
    if not m:
        return None
    try:
        mo, dy = int(m.group(1)), int(m.group(2))
        # 年またぎ考慮（12月に翌年1-3月の受渡し）
        if date.today().month >= 11 and mo <= 3:
            year += 1
        return date(year, mo, dy).isoformat()
    except Exception:
        return None

def parse_jp_date_range_end(text: str) -> str | None:
    """'4月1日(水) ～ 4月6日(月)' → 先頭の日付 '2025-04-01'"""
    all_dates = re.findall(r'(\d{1,2})月(\d{1,2})日', text)
    if not all_dates:
        return None
    mo, dy = int(all_dates[0][0]), int(all_dates[0][1])
    yr = THIS_YEAR
    if date.today().month >= 11 and mo <= 3:
        yr += 1
    try:
        return date(yr, mo, dy).isoformat()
    except Exception:
        return None

def load_records() -> list:
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, encoding="utf-8") as f:
        raw = json.load(f)
    return raw.get("records", raw) if isinstance(raw, dict) else raw

def save_records(records: list):
    os.makedirs("data", exist_ok=True)
    out = {"records": records, "last_updated": datetime.now().isoformat(), "count": len(records)}
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"保存完了: {len(records)} 件")

# ── pokabu.net スクレイピング ─────────────────────────────────────────────────

def scrape_schedule() -> dict:
    """
    /schedule ページから取得:
    pending[code]   = {code, name, article_url, decision_date, lending_type}
    delivered[code] = {code, name, article_url, delivery_date, issue_price}
    """
    try:
        res = requests.get(f"{BASE_URL}/schedule", headers=HEADERS, timeout=20)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
    except Exception as e:
        print(f"スケジュール取得エラー: {e}")
        return {"pending": {}, "delivered": {}}

    pending   = {}
    delivered = {}

    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue
        header = rows[0].get_text(" ", strip=True)
        is_dec = "決定日" in header or "価格等" in header
        is_del = "受渡日" in header and "価格等" not in header

        for row in rows[1:]:
            cells    = row.find_all("td")
            row_text = row.get_text(" ", strip=True)
            code_m   = re.search(r'\b(\d{4})\b', row_text)
            if not code_m:
                continue
            code  = code_m.group(1)
            link  = row.find("a", href=re.compile(r'/po/'))
            name  = link.get_text(strip=True) if link else ""
            _href = link["href"] if link else None
            a_url = (_href if _href and _href.startswith("http") else (BASE_URL + _href if _href else None))

            if is_dec:
                date_text = cells[0].get_text(" ", strip=True) if cells else ""
                dec_date  = parse_jp_date_range_end(date_text)
                lending   = ""
                for cell in cells:
                    ct = cell.get_text(strip=True)
                    if ct in ("貸借", "信用"):
                        lending = ct
                        break
                pending[code] = {"code": code, "name": name, "article_url": a_url,
                                 "decision_date": dec_date, "lending_type": lending}

            elif is_del:
                date_text   = cells[0].get_text(" ", strip=True) if cells else ""
                del_date    = parse_jp_date(date_text)
                price_text  = cells[-1].get_text(strip=True) if cells else ""
                price_m     = re.search(r'([\d,]+)円', price_text)
                issue_price = int(price_m.group(1).replace(",", "")) if price_m else None
                delivered[code] = {"code": code, "name": name, "article_url": a_url,
                                   "delivery_date": del_date, "issue_price": issue_price}

    print(f"スケジュール: 決定日前 {len(pending)} 件 / 受渡日前 {len(delivered)} 件")
    return {"pending": pending, "delivered": delivered}


def scrape_article(url: str, name: str = "", code: str = "") -> dict:
    """
    個別記事ページから詳細情報を取得。
    増資数  = 新株発行 + 自己株式処分（自己株式処分を含む）
    売出し数 = 売出株数 + OA売出（OA含む）
    """
    info = {}
    try:
        res = requests.get(url, headers=HEADERS, timeout=20)
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")
        # 記事本文エリアを絞り込む（サイドバー・関連記事の混入防止）
        main_elem = (soup.find("article") or
                     soup.find("main") or
                     soup.find("div", id=re.compile(r'content|article|main', re.I)) or
                     soup.find("div", class_=re.compile(r'content|article|post|entry', re.I)) or
                     soup)
        full_text = main_elem.get_text(" ", strip=True)
    except Exception as e:
        print(f"  記事取得エラー ({url}): {e}")
        return info

    # 記事公開日（meta タグ）をフォールバック用に取得
    pub_meta = soup.find("meta", {"property": "article:published_time"}) or soup.find("meta", {"name": "pubdate"})
    if pub_meta and pub_meta.get("content"):
        info["article_published"] = pub_meta["content"][:10]

    # 種類判定
    info["type"] = "リート" if (re.match(r'\d{4}F$', code or "") or any(k in (name or "") for k in ["リート", "投資法人"])) else "普通"

    # PO規模（億円）— 記事本文: "最大157億円規模" / "463億円規模"
    scale_m = re.search(r'(?:最大|合計)?(\d+(?:\.\d+)?)億円規模', full_text)
    if scale_m:
        info["po_scale"] = float(scale_m.group(1).replace(",", ""))

    # 株数パーツを個別に集積してから合算する
    _new_shares      = 0   # 新株発行（公募）
    _treasury_shares = 0   # 自己株式処分
    _sold_shares     = 0   # 売出株数（OA除く）
    _oa_shares       = 0   # OA売出

    def _parse_shares(val: str) -> int:
        """数字を含む文字列から株数を取り出す。"未定" / "0株" は 0 を返す。"""
        if "未定" in val:
            return 0
        val_clean = val.replace(",", "").replace(" ", "")
        m = re.search(r'(\d+)', val_clean)
        return int(m.group(1)) if m else 0

    for table in soup.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) < 2:
                continue
            key = cells[0].get_text(strip=True)
            val = cells[1].get_text(strip=True)

            if "発表日" in key or "公表日" in key:
                d = parse_jp_date(val)
                if d:
                    info["announce_date"] = d

            elif "時価総額" in key:
                m = re.search(r'([\d,]+)億', val)
                if m:
                    info["market_cap"] = int(m.group(1).replace(",", ""))

            elif "価格決定日" in key or "条件決定日" in key:
                d = parse_jp_date_range_end(val)
                if d and not info.get("decision_date"):
                    info["decision_date"] = d

            elif "受渡予定日" in key:
                d = parse_jp_date(val)
                if d:
                    info["delivery_estimated"] = d

            elif "受渡日" in key and "予定" not in key:
                d = parse_jp_date(val)
                if d:
                    info["delivery_date"] = d

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

            # ── 株数系（個別に収集）────────────────────────────────────
            elif any(k in key for k in ["新株発行", "新投資口発行"]):
                # 自己株式処分が同じ行に書かれるケースを除外
                if "自己株" not in key:
                    _new_shares = _parse_shares(val)

            elif "自己株式処分" in key or "自己株" in key:
                _treasury_shares = _parse_shares(val)

            elif any(k in key for k in ["売出株数", "売出口数", "投資口売出"]):
                # OA行は別途取るので純粋売出しのみ
                if "OA" not in key and "オーバー" not in key:
                    _sold_shares = _parse_shares(val)

            elif any(k in key for k in ["OA売出", "オーバーアロット", "第三者割当"]):
                _oa_shares = _parse_shares(val)

    # 合算
    new_total  = _new_shares + _treasury_shares   # 増資数（自己株式処分を含む）
    sold_total = _sold_shares + _oa_shares         # 売出し数（OA含む）

    if new_total  > 0: info["new_shares"]  = new_total
    if sold_total > 0: info["sold_shares"] = sold_total

    # 内訳も保持（デバッグ・参照用）
    if _treasury_shares > 0: info["treasury_shares"] = _treasury_shares
    if _oa_shares       > 0: info["oa_shares"]       = _oa_shares

    # 記事本文から確定価格・割引率をフォールバック取得
    if not info.get("issue_price"):
        pm = re.search(r'(?:発行|処分)価格は([\d,]+)円に決定', full_text)
        if pm:
            info["issue_price"] = int(pm.group(1).replace(",", ""))
    if not info.get("discount_rate"):
        dm = re.search(r'割引率は([\d.]+)[％%]', full_text)
        if dm:
            info["discount_rate"] = float(dm.group(1))

    # ── 幹事テーブルを取得 ────────────────────────────────────────────────
    # pokabu.net 個別記事の「幹事」セクション:
    #   | 主幹事 | 野村證券 |
    #   | 引受人 | 大和証券 |  など
    lead_managers  = []   # 主幹事
    co_managers    = []   # 引受人

    for table in soup.find_all("table"):
        header_text = table.get_text(" ", strip=True)
        # 幹事テーブルかどうかを判定（「主幹事」または「証券会社」という文字が含まれる）
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
            elif any(k in role for k in ["引受", "委託", "副幹事"]):
                co_managers.append(name)

    if lead_managers: info["lead_managers"] = lead_managers
    if co_managers:   info["co_managers"]   = co_managers

    return info


# ── Yahoo Finance JP 株価取得 ─────────────────────────────────────────────────

def fetch_prices(code: str, days: int = 60) -> tuple:
    """
    Returns: (prices_dict, market_cap_oku, shares_outstanding)
      prices_dict       : {date_str: {open, close, high}}
      market_cap_oku    : 時価総額（億円）int | None
      shares_outstanding: 発行済み株式数 int | None
    """
    ticker = f"{code}.T"
    url    = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range={days}d"
    try:
        res = requests.get(url, headers=HEADERS, timeout=12)
        if res.status_code != 200:
            return {}, None, None
        data   = res.json()
        result = data.get("chart", {}).get("result")
        if not result:
            return {}, None, None
        r    = result[0]
        q    = r["indicators"]["quote"][0]
        meta = r.get("meta", {})
        tss  = r.get("timestamp", [])
        prices = {}
        for i, ts in enumerate(tss):
            if not ts:
                continue
            d = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
            prices[d] = {
                "open":  round(q["open"][i],  2) if q["open"][i]  else None,
                "close": round(q["close"][i], 2) if q["close"][i] else None,
                "high":  round(q["high"][i],  2) if q["high"][i]  else None,
            }
        mc     = meta.get("marketCap")
        shares = meta.get("sharesOutstanding")
        return prices, (round(mc / 1e8) if mc else None), (int(shares) if shares else None)
    except Exception as e:
        print(f"  Yahoo Finance エラー ({code}): {e}")
        return {}, None, None


def update_prices(rec: dict) -> dict:
    code    = rec.get("code")
    ann_str = rec.get("announce_date", "")
    if not code or not ann_str:
        return rec
    try:
        ann_date = datetime.fromisoformat(ann_str).date()
    except Exception:
        return rec

    prices, mc, shares_outstanding = fetch_prices(code)
    if not prices:
        return rec

    # 時価総額（記事から取れなかった場合のみ補完）
    if not rec.get("market_cap") and mc:
        rec["market_cap"] = mc

    # 発行済み株式数（shares_outstanding）
    if not rec.get("shares_outstanding") and shares_outstanding:
        rec["shares_outstanding"] = shares_outstanding
        print(f"  {rec['name']}: 発行済み株式数 = {shares_outstanding:,}")

    # フォールバック: Yahoo Financeが株式数を返さない場合、時価総額÷株価で推計
    if not rec.get("shares_outstanding") and rec.get("market_cap") and prices:
        latest_close = next(
            (prices[d]["close"] for d in sorted(prices, reverse=True) if prices[d].get("close")),
            None
        )
        if latest_close:
            estimated = int(rec["market_cap"] * 1e8 / latest_close)
            rec["shares_outstanding"] = estimated
            print(f"  {rec['name']}: 発行済み株式数（推計）= {estimated:,}")

    # 希薄化率 = 増資数（new_shares） ÷ 発行済み株式数 × 100
    # 増資数が入っていてかつ発行済み株式数が確定したタイミングで計算
    if (not rec.get("dilution")
            and rec.get("new_shares")
            and rec.get("shares_outstanding")):
        rec["dilution"] = round(
            rec["new_shares"] / rec["shares_outstanding"] * 100, 1
        )
        print(f"  {rec['name']}: 希薄化率 = {rec['dilution']}%")

    next_day     = next_biz_day(ann_date)
    next_day_str = next_day.isoformat()

    # 翌日始値
    if not rec.get("next_open") and next_day_str in prices:
        p = prices[next_day_str]
        if p["open"]:
            rec["next_open"] = p["open"]
            print(f"  {rec['name']}: 翌日始値 = {rec['next_open']:,}")

    # 翌日以降の最高値（決定日まで）
    if rec.get("next_open"):
        dec_date = rec.get("decision_date", "")
        max_p    = rec.get("max_price") or 0
        for d_str in sorted(prices):
            # 決定日が確定していれば決定日まで、未定なら全期間で最高値を追う
            if d_str < next_day_str:
                continue
            if dec_date and d_str > dec_date:
                break
            if prices[d_str]["high"]:
                max_p = max(max_p, prices[d_str]["high"])
        if max_p:
            rec["max_price"]   = max_p
            rec["open_to_max"] = round((max_p - rec["next_open"]) / rec["next_open"] * 100, 2)

    # 受渡日が確定している場合は逆算で決定日を上書き（受渡日の3営業日前）
    # → pokabu.netの「初日推定」より正確な決定日になる
    confirmed_del = rec.get("delivery_date")  # scheduleの受渡日テーブルから取得した確定値
    if confirmed_del and not rec.get("decision_date_confirmed"):
        try:
            del_d    = datetime.fromisoformat(confirmed_del).date()
            calc_dec = prev_biz_days(del_d, 3)
            calc_dec_str = calc_dec.isoformat()
            if rec.get("decision_date") != calc_dec_str:
                print(f"  {rec['name']}: 決定日を受渡日逆算で修正 {rec.get('decision_date')} → {calc_dec_str}")
                rec["decision_date"]           = calc_dec_str
                rec["decision_date_confirmed"] = True
                # 修正に伴い既存の価格データをリセットして再取得
                rec["dec_open"]  = None
                rec["dec_close"] = None
                rec["ret_open"]  = None
                rec["ret_close"] = None
        except Exception as e:
            print(f"  決定日逆算エラー: {e}")

    # 決定日 → 騰落率計算
    dec_date = rec.get("decision_date")
    if dec_date and dec_date in prices and not rec.get("dec_open"):
        p = prices[dec_date]
        if p["open"] and p["close"]:
            rec["dec_open"]  = p["open"]
            rec["dec_close"] = p["close"]

    # 騰落率が未計算なら算出（dec_open/closeが先に入っていたケースも救済）
    if rec.get("dec_open") and rec.get("next_open") and not rec.get("ret_open"):
        rec["ret_open"]  = round((rec["dec_open"]  - rec["next_open"]) / rec["next_open"] * 100, 2)
        rec["ret_close"] = round((rec["dec_close"] - rec["next_open"]) / rec["next_open"] * 100, 2)
        print(f"  {rec['name']}: 騰落率(始){rec['ret_open']}% 騰落率(終){rec['ret_close']}%")

    # 受渡日 → 寄り・大引け・騰落率（A=受渡始値, B=受渡終値, C=B÷A）
    del_date = rec.get("delivery_date") or rec.get("delivery_estimated")
    if del_date and del_date in prices and not rec.get("delivery_open"):
        p = prices[del_date]
        if p["open"] and p["close"]:
            rec["delivery_open"]  = p["open"]
            rec["delivery_close"] = p["close"]
            rec["delivery_ret"]   = round((p["close"] - p["open"]) / p["open"] * 100, 2)
            print(f"  {rec['name']}: 受渡日 始値={p['open']:,} 終値={p['close']:,} 騰落率={rec['delivery_ret']}%")

    # 完了判定は受渡日の価格が取得できた時点
    rec["status"] = ("complete" if rec.get("delivery_open") and rec.get("delivery_close")
                     else "nextday" if rec.get("next_open")
                     else "pending")
    return rec


# ── RSS フィード ─────────────────────────────────────────────────────────────

def scrape_rss() -> list:
    """pokabu.net/feed から最新PO記事を取得し、銘柄コードとURLを返す"""
    entries = []
    try:
        res = requests.get(f"{BASE_URL}/feed", headers=HEADERS, timeout=15)
        if res.status_code != 200:
            print(f"  RSS取得失敗: HTTP {res.status_code}")
            return entries
        res.encoding = "utf-8"
        soup = BeautifulSoup(res.text, "xml" if "xml" in res.headers.get("content-type","") else "html.parser")
        for item in soup.find_all("item"):
            title = item.find("title")
            link = item.find("link")
            if not title or not link:
                continue
            title_text = title.get_text(strip=True)
            link_text = link.get_text(strip=True) if link.string else (link.next_sibling or "").strip()
            if not link_text and link.get("href"):
                link_text = link["href"]
            code_m = re.search(r'[（(](\d{4})[）)]', title_text)
            if code_m and "/po/" in (link_text or "") and not any(p in link_text for p in ("-kansoku", "-yotei", "-kabuka")):
                entries.append({"code": code_m.group(1), "title": title_text, "url": link_text})
    except Exception as e:
        print(f"  RSSエラー: {e}")
    return entries


# ── メイン ────────────────────────────────────────────────────────────────────

def lending_to_alert(lending: str) -> str:
    """貸借→空 / 信用→注意 / なし→売り禁"""
    return "" if lending == "貸借" else ("注意" if lending == "信用" else "売り禁")


def main():
    today = date.today().isoformat()
    print(f"\n{'='*50}\nPO Tracker 実行: {today}\n{'='*50}\n")

    records  = load_records()
    existing = {r["code"]: r for r in records if r.get("code")}

    # ⓪ RSS フィードから新規PO検知
    print("[0] pokabu.net/feed チェック...")
    rss_entries = scrape_rss()
    rss_new = 0
    for entry in rss_entries:
        code = entry["code"]
        if code in existing:
            continue
        print(f"  RSS新規検知: {entry['title']}")
        article = scrape_article(entry["url"], name="", code=code)
        time.sleep(1)
        if not article:
            continue
        name_m = re.search(r'[】](.*?)[（(]', entry["title"])
        name = name_m.group(1).strip() if name_m else ""
        lending = article.get("lending_type", "")
        announce = article.get("announce_date") or article.get("article_published") or today
        new_rec = {
            "id":                 f"{code}_{announce.replace('-','')}",
            "code":               code,
            "name":               name,
            "type":               article.get("type", "普通"),
            "alert":              lending_to_alert(lending),
            "lending_type":       lending,
            "announce_date":      announce,
            "announce_date_confirmed": bool(article.get("announce_date")),
            "year":               THIS_YEAR,
            "decision_date":      article.get("decision_date"),
            "decision_date_confirmed": bool(article.get("decision_date")),
            "delivery_estimated": article.get("delivery_estimated"),
            "delivery_date":      article.get("delivery_date"),
            "issue_price":        article.get("issue_price"),
            "discount_range":     article.get("discount_range"),
            "discount_rate":      article.get("discount_rate"),
            "market_cap":         article.get("market_cap"),
            "po_scale":           article.get("po_scale"),
            "new_shares":         article.get("new_shares"),
            "treasury_shares":    article.get("treasury_shares"),
            "sold_shares":        article.get("sold_shares"),
            "oa_shares":          article.get("oa_shares"),
            "shares_outstanding": None,
            "lead_managers":      article.get("lead_managers", []),
            "co_managers":        article.get("co_managers", []),
            "article_url":        entry["url"],
            "po_pct":             None,
            "dilution":           article.get("dilution"),
            "next_open": None, "max_price": None, "open_to_max": None,
            "dec_open": None, "dec_close": None,
            "ret_open": None, "ret_close": None,
            "delivery_open": None, "delivery_close": None, "delivery_ret": None,
            "memo": "", "status": "pending",
        }
        if new_rec["po_scale"] and new_rec["market_cap"]:
            new_rec["po_pct"] = round(new_rec["po_scale"] / new_rec["market_cap"] * 100, 1)
        records.append(new_rec)
        existing[code] = new_rec
        rss_new += 1
    print(f"  RSS新規: {rss_new} 件\n")

    # ① スケジュール取得
    print("[1] pokabu.net/schedule 取得中...")
    sched    = scrape_schedule()
    pending  = sched["pending"]
    delivered = sched["delivered"]

    added = []

    for code, si in {**pending, **delivered}.items():
        if code in existing:
            rec = existing[code]
            # 既存レコードへの補完
            if not rec.get("decision_date") and si.get("decision_date"):
                rec["decision_date"] = si["decision_date"]
            if code in delivered:
                di = delivered[code]
                if not rec.get("delivery_date") and di.get("delivery_date"):
                    rec["delivery_date"] = di["delivery_date"]
                if not rec.get("issue_price") and di.get("issue_price"):
                    rec["issue_price"] = di["issue_price"]
            # announce_date が未確認なら記事から正しい発表日を取得
            if rec.get("article_url") and not rec.get("announce_date_confirmed"):
                print(f"  発表日確認: {rec.get('name')} ({code})")
                a_info = scrape_article(rec["article_url"], name=rec.get("name",""), code=code or "")
                time.sleep(0.8)
                real_date = a_info.get("announce_date") or a_info.get("article_published")
                if real_date and real_date != rec.get("announce_date"):
                    old = rec.get("announce_date")
                    rec["announce_date"] = real_date
                    rec["id"] = f"{code}_{real_date.replace('-','')}"
                    # 日付が変わったので株価データをリセット（次回再取得）
                    rec["next_open"] = None
                    rec["dec_open"] = None
                    rec["dec_close"] = None
                    rec["ret_open"] = None
                    rec["ret_close"] = None
                    rec["decision_date_confirmed"] = False
                    print(f"    発表日修正: {old} → {real_date}（株価リセット）")
                rec["announce_date_confirmed"] = True
                # 補完データも更新
                for field in ["type", "po_scale", "market_cap", "new_shares", "treasury_shares",
                              "sold_shares", "oa_shares", "discount_range", "discount_rate",
                              "delivery_estimated", "lead_managers", "co_managers", "dilution"]:
                    if a_info.get(field) and not rec.get(field):
                        rec[field] = a_info[field]
                if rec.get("po_scale") and rec.get("market_cap"):
                    rec["po_pct"] = round(rec["po_scale"] / rec["market_cap"] * 100, 1)

            # 記事データが未取得の場合は再スクレイピング
            elif rec.get("article_url") and (not rec.get("po_scale") and not rec.get("new_shares")
                                            or not rec.get("dilution")):
                print(f"  記事再取得: {rec.get('name')} ({code})")
                article = scrape_article(rec["article_url"], name=rec.get("name",""), code=rec.get("code","") or "")
                time.sleep(0.8)
                if article:
                    for field in ["type", "po_scale", "market_cap", "new_shares", "treasury_shares",
                                  "sold_shares", "oa_shares", "discount_range", "discount_rate",
                                  "delivery_estimated", "lead_managers", "co_managers", "dilution"]:
                        if article.get(field) and not rec.get(field):
                            rec[field] = article[field]
                    if rec.get("po_scale") and rec.get("market_cap"):
                        rec["po_pct"] = round(rec["po_scale"] / rec["market_cap"] * 100, 1)
            continue

        # 新規 → 記事から詳細取得
        article = {}
        if si.get("article_url"):
            print(f"  記事取得: {si.get('name')} ({code})")
            article = scrape_article(si["article_url"], name=si.get("name",""), code=code or "")
            time.sleep(0.8)

        lending = si.get("lending_type", "")
        actual_announce = article.get("announce_date") or article.get("article_published") or today
        new_rec = {
            "id":                 f"{code}_{actual_announce.replace('-','')}",
            "code":               code,
            "name":               si.get("name", ""),
            "type":               article.get("type", "普通"),
            "alert":              lending_to_alert(lending),
            "lending_type":       lending,
            "announce_date":      actual_announce,
            "announce_date_confirmed": bool(article.get("announce_date")),
            "year":               THIS_YEAR,
            "decision_date":      article.get("decision_date") or si.get("decision_date"),
            "delivery_estimated": article.get("delivery_estimated"),
            "delivery_date":      delivered.get(code, {}).get("delivery_date") or article.get("delivery_date"),
            "issue_price":        delivered.get(code, {}).get("issue_price")   or article.get("issue_price"),
            "discount_range":     article.get("discount_range"),
            "discount_rate":      article.get("discount_rate"),
            "market_cap":         article.get("market_cap"),
            "po_scale":           article.get("po_scale"),
            "new_shares":         article.get("new_shares"),        # 増資数（自己株式処分含む）
            "treasury_shares":    article.get("treasury_shares"),   # うち自己株式処分分
            "sold_shares":        article.get("sold_shares"),        # 売出し数（OA含む）
            "oa_shares":          article.get("oa_shares"),          # うちOA分
            "shares_outstanding": None,                              # 発行済み株式数（Yahoo Finance）
            "lead_managers":      article.get("lead_managers", []),    # 主幹事リスト
            "co_managers":        article.get("co_managers",   []),    # 引受人リスト
            "article_url":        si.get("article_url"),
            "po_pct":             None,   # PO規模割合（自動計算）
            "dilution":           article.get("dilution"),   # 希薄化率（記事から取得）
            "next_open":          None,
            "max_price":          None,
            "open_to_max":        None,
            "dec_open":           None,
            "dec_close":          None,
            "ret_open":           None,
            "ret_close":          None,
            "delivery_open":      None,   # 受渡日の寄り価格（A）
            "delivery_close":     None,   # 受渡日の大引け価格（B）
            "delivery_ret":       None,   # 受渡日騰落率（C=B÷A）
            "memo":               "",
            "status":             "pending",
        }
        # PO規模割合 = 規模 ÷ 時価総額 × 100
        if new_rec["po_scale"] and new_rec["market_cap"]:
            new_rec["po_pct"] = round(new_rec["po_scale"] / new_rec["market_cap"] * 100, 1)

        records.append(new_rec)
        existing[code] = new_rec
        added.append(new_rec)
        print(f"  + {new_rec['name']} ({code}) 規模:{new_rec.get('po_scale','?')}億 "
              f"時価総額:{new_rec.get('market_cap','?')}億 決定日:{new_rec.get('decision_date','?')} "
              f"受渡:{new_rec.get('delivery_estimated','?')} 注意:{new_rec.get('alert','') or 'なし'}")

    # ② 株価更新
    print(f"\n[2] 株価更新中 ({len(records)} 件)...")
    newly_done = []
    updated    = []

    for rec in records:
        was_complete = rec.get("status") == "complete"
        try:
            elapsed = (date.today() - datetime.fromisoformat(rec.get("announce_date", today)).date()).days
        except Exception:
            elapsed = 0
        if was_complete and elapsed > 45:
            updated.append(rec)
            continue
        print(f"  [{rec.get('status','?')}] {rec['name']} ({rec.get('code','?')})")
        rec = update_prices(rec)
        updated.append(rec)
        if not was_complete and rec.get("status") == "complete":
            newly_done.append(rec)
        time.sleep(0.3)

    # ③ 完了ログ
    for r in newly_done:
        so = "+" if (r.get("ret_open")  or 0) >= 0 else ""
        sc = "+" if (r.get("ret_close") or 0) >= 0 else ""
        print(f"  ✓ 完了: {r['name']} ({r.get('code','')}) "
              f"始:{so}{r.get('ret_open','?')}% 終:{sc}{r.get('ret_close','?')}%")

    save_records(updated)
    pending_cnt = sum(1 for r in updated if r.get("status") != "complete")
    print(f"\n完了: {len(updated)} 件 / 未完了: {pending_cnt} 件")


if __name__ == "__main__":
    main()
