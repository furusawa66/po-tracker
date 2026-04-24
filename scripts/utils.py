"""共通ユーティリティ: 祝日判定 / 営業日 / 日付パース / 原子的書き込み / HTTP リトライ"""

import json, os, re, tempfile, time
from datetime import datetime, date, timedelta

try:
    import jpholiday
except ImportError:
    jpholiday = None


def is_jp_holiday(d: date) -> bool:
    """東証休場日判定: 国民の祝日 + 年末年始休場（12/31, 1/2, 1/3）"""
    if (d.month == 12 and d.day == 31) or (d.month == 1 and d.day in (2, 3)):
        return True
    if jpholiday:
        return jpholiday.is_holiday(d)
    return d.month == 1 and d.day == 1


def next_biz_day(d: date) -> date:
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5 or is_jp_holiday(nd):
        nd += timedelta(days=1)
    return nd


def prev_biz_day(d: date) -> date:
    pd = d - timedelta(days=1)
    while pd.weekday() >= 5 or is_jp_holiday(pd):
        pd -= timedelta(days=1)
    return pd


def prev_biz_days(d: date, n: int) -> date:
    """n営業日前（祝日対応）"""
    result = d
    for _ in range(n):
        result = prev_biz_day(result)
    return result


def _adjust_year_for_wrap(mo: int, year: int) -> int:
    """年またぎを推定して年を補正:
    11-12月時点で1-3月の日付 → 翌年扱い
    1-3月時点で11-12月の日付  → 前年扱い"""
    cur_mo = date.today().month
    if cur_mo >= 11 and mo <= 3:
        return year + 1
    if cur_mo <= 3 and mo >= 11:
        return year - 1
    return year


def parse_jp_date(text: str, year: int = None) -> str | None:
    """'4月6日' '4月6日(月)' → 'YYYY-MM-DD'"""
    if not year:
        year = date.today().year
    m = re.search(r'(\d{1,2})月(\d{1,2})日', text)
    if not m:
        return None
    try:
        mo, dy = int(m.group(1)), int(m.group(2))
        return date(_adjust_year_for_wrap(mo, year), mo, dy).isoformat()
    except Exception:
        return None


def parse_jp_date_range_end(text: str) -> str | None:
    """'4月1日(水) ～ 4月6日(月)' → 先頭の日付 'YYYY-MM-DD'"""
    all_dates = re.findall(r'(\d{1,2})月(\d{1,2})日', text)
    if not all_dates:
        return None
    mo, dy = int(all_dates[0][0]), int(all_dates[0][1])
    try:
        return date(_adjust_year_for_wrap(mo, date.today().year), mo, dy).isoformat()
    except Exception:
        return None


def atomic_write_json(path: str, data) -> None:
    """同一ディレクトリに一時ファイル → fsync → rename で原子的に書き込む"""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", suffix=".json", dir=d)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def http_get_with_retry(url: str, headers: dict = None, timeout: int = 15,
                        max_retries: int = 4, backoff_base: float = 2.0):
    """HTTP GET をリトライ付きで実行。429/5xx/接続エラーで指数バックオフ。
    成功時 Response を返し、最終失敗時 None を返す。
    KeyboardInterrupt や SystemExit は伝播させる（ユーザー中断を妨げない）。"""
    import requests
    for attempt in range(max_retries):
        try:
            res = requests.get(url, headers=headers, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError):
            if attempt < max_retries - 1:
                time.sleep(backoff_base ** attempt)
            continue
        except requests.RequestException:
            return None  # その他の requests エラーはリトライしない
        if res.status_code == 429 or 500 <= res.status_code < 600:
            wait = backoff_base ** attempt
            ra = res.headers.get("Retry-After")
            if ra:
                try: wait = max(wait, float(ra))
                except ValueError: pass
            if attempt < max_retries - 1:
                time.sleep(wait)
            continue
        return res
    return None
