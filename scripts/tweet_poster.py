#!/usr/bin/env python3
"""
X (Twitter) 自動投稿スクリプト
note記事の内容をもとにClaude APIでツイートを生成して投稿する
"""

import os
import random
import anthropic
import tweepy
from datetime import datetime

# =====================
# 売りたいnote記事
# =====================
PAID_NOTE = {
    "title": "【アフィ裏技】中古ドメイン「co.jp」を見つける方法２つ",
    "url": "https://note.com/affiliate_note/n/na689ee7abbc9",
    "price": "39,800円",
}

# =====================
# 無料記事から拾ったネタ（フック用）
# =====================
FREE_CONTENT_HOOKS = [
    {
        "theme": "引き算SEO",
        "hook": "コンテンツを増やしてもSEOが上がらない時代。「引き算SEO」という考え方が2026年は刺さる。不要な情報を削り、検索意図に絞る。加点より減点しない戦略。",
    },
    {
        "theme": "検索結果の並び順",
        "hook": "Googleの検索結果の「並び順」を見るだけでコンテンツ優先度がわかる。1位がYouTubeなら動画を作れ、ショッピングなら商品ページを作れ。Googleが答えを教えてくれてる。",
    },
    {
        "theme": "Whois公開",
        "hook": "Whois情報を隠すとSEOで損する。Googleは「誰が運営しているか」を重視している。プライバシーより権威性の積み上げ。個人サイトこそ公開すべき理由がある。",
    },
    {
        "theme": "営業メール活用",
        "hook": "うざい営業メールを捨てずにSEOに使う。URL付きで返信するだけ。地味な加点だが無料でできる。アフィリエイターは使えるものは何でも使う。",
    },
    {
        "theme": "競合の中古ドメインを落とす",
        "hook": "競合サイトが中古ドメインを使っていたら、合法的にSEO順位を落とせる。被リンクを削る＝ドメインパワーを削る。知ってるかどうかで差がつく。",
    },
    {
        "theme": "co.jpドメインの希少性",
        "hook": ".co.jpドメインは法人しか取れない。だから中古が出回ると価値が高い。個人アフィリエイターがこれを使えたら大手と戦える。その見つけ方を公開している。",
    },
    {
        "theme": "2026年の個人アフィ",
        "hook": "2026年、個人アフィリエイターが大手に勝てる手段は限られてきた。その数少ない手段のひとつが中古の.co.jpドメイン。専業7年が実践してきた方法。",
    },
    {
        "theme": "A8ランクSS",
        "hook": "A8ネットのランクSSって何者？月500万超えのアフィ専業が使ってきたSEO手法を有料noteで公開中。ブラック手法も使ってきたからこそわかる本当に効く手段。",
    },
    {
        "theme": "SEO監修",
        "hook": "SEO監修者を安く使う方法がある。外注を安く使えれば、その分を中古ドメイン取得に回せる。勝つアフィリエイターは資源配分がうまい。",
    },
    {
        "theme": "カニバリゼーション",
        "hook": "GRCとサチコで重複記事（カニバリ）を見つける方法がある。放置するとSEOに悪影響。中古ドメインを使う前に自サイトの内部整理も必要。",
    },
]

HASHTAGS = "#SEO #アフィリエイト #中古ドメイン #note #アフィ"


def should_include_url() -> bool:
    """3日に1回（月・木）だけnote URLを含める"""
    return datetime.now().weekday() in (0, 3)  # 0=月曜, 3=木曜


def generate_tweet(include_url: bool) -> str:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    hook = random.choice(FREE_CONTENT_HOOKS)
    today = datetime.now().strftime("%Y-%m-%d %H:%M")

    if include_url:
        cta = f"""
- 最後に自然な形でnoteへの誘導を1文入れる
- 文末に必ずこのURLとハッシュタグをつける:
  {PAID_NOTE["url"]}
  {HASHTAGS}"""
    else:
        cta = f"""
- note URLは含めない
- 純粋に価値ある情報として投稿する
- 文末にハッシュタグのみつける: {HASHTAGS}"""

    prompt = f"""
あなたはSEOアフィリエイト専業7年のプロです。
以下の情報をもとに、Xに投稿するツイートを1つ作成してください。

【今日のフック】
テーマ: {hook["theme"]}
素材: {hook["hook"]}

【ツイート作成ルール】
- 本文（URLとハッシュタグを除く）は150字以内
- ターゲットはSEOアフィリエイターの中〜上級者
- 押し売り感を出さず、具体的な価値を伝える
- 毎回違うパターンで書く（日時: {today}）
{cta}
- ツイート本文のみ出力（前置きや説明は不要）
"""

    message = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )

    return message.content[0].text.strip()


def post_tweet(text: str) -> None:
    client = tweepy.Client(
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_TOKEN_SECRET"],
    )
    response = client.create_tweet(text=text)
    print(f"投稿成功 (id: {response.data['id']})")
    print(f"---\n{text}\n---")


if __name__ == "__main__":
    include_url = should_include_url()
    print(f"ツイート生成中... (URL含む: {include_url})")
    tweet = generate_tweet(include_url)
    print(f"\n生成されたツイート:\n{tweet}\n")
    post_tweet(tweet)
