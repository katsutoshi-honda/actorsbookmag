# actorsbookmag 運用・引き継ぎドキュメント

自主・独立映画のためのカルチャーメディア **ACTORSBOOK** の運用手順・構成・復旧手順をまとめる。

- 本番URL: https://actorsbookmag.vercel.app/ （`/` は `/news/` へ転送）
- GitHub: https://github.com/katsutoshi-honda/actorsbookmag
- ホスティング: Vercel（`main` への push で自動デプロイ）

---

## 1. サイトの入口

2026-08 以降、**サイトの入口は `/news`（ACTORSBOOK）に一本化**した。

| URL | 内容 |
|---|---|
| `/` | `/news/` へ **307 一時リダイレクト**（`vercel.json`） |
| `/news/` | ACTORSBOOK トップ（記事一覧）。**メインの入口** |
| `/news/archive.html` | アーカイブ |
| `/news/article.html?id=…` | 記事個別ページ |
| `/news/admin.html` | CMS（管理画面）。後述のパスワードでログイン |

### 旧TOP（FRAME）について
ルートの `index.html` / `indie-film-site.html` は旧TOP「FRAME」。**ファイルは残しているが導線からは外した**（`/` に来ても `/news/` へ飛ぶため実質非公開）。復活させたい場合は `vercel.json` の `redirects` を削除すれば `/` で再び表示される。

- `frame-dashboard.html` / `frame-projects.html` / `indie-film-system-roadmap.html` / `streaming.html` も旧FRAME系の残置ファイル。直リンクでのみ到達可能。

> リダイレクトを **一時（`permanent: false` = 307）** にしているのは、将来 `/` に新しいTOPを置く可能性を残すため。SEOで恒久統合したくなったら `permanent: true`（308）に変更する。

---

## 2. リポジトリ構成

```
actorsbookmag/
├── vercel.json              # / → /news/ リダイレクト設定
├── index.html               # 旧TOP(FRAME)。導線外。残置
├── indie-film-site.html     # index.html と同内容
├── streaming.html           # 旧: MUBI/Filmarks 新着フィルター
├── frame-*.html             # 旧FRAME系ページ
│
├── news/                    # ★現行サイト本体 (ACTORSBOOK)
│   ├── index.html           #   トップ（/data/news.json を fetch して一覧描画）
│   ├── article.html         #   記事個別
│   ├── archive.html         #   アーカイブ
│   ├── admin.html           #   CMS（管理画面）
│   └── style.css
│
├── api/
│   └── save.js              # Vercel Serverless Function。CMSの保存API
│
├── data/
│   ├── news.json            # ★記事データ（サイトの表示ソース）
│   ├── films.json           # 配信新着映画データ（Actionsが毎朝更新）
│   ├── banners/             # 監督バナー画像（Actionsが生成）
│   └── taste_profile.json
│
├── images/news/             # 記事サムネ・背景画像（CMSがコミット）
├── social/                  # 生成されたSNS素材（IG/TikTok/Reels）
├── output/                  # ローカル生成物（.gitignore対象）
│
├── scripts/
│   ├── pipeline.py          # ★記事自動生成パイプライン (ideas / make)
│   ├── fetch_films.py       # 配信新着映画スクレイピング
│   ├── generate_banners.py  # 監督バナー生成
│   ├── make_assets.py       # SNS素材(IGカルーセル/縦型動画)生成
│   └── post_x.py            # X(Twitter)自動投稿
│
└── .github/workflows/
    ├── fetch_films.yml      # 毎朝 06:17 JST に映画取得＋バナー生成
    ├── make_assets.yml      # news.json 更新時にSNS素材生成
    └── post_x.yml           # news.json 更新時にXへ投稿（※未コミットの場合あり・後述）
```

### データフロー
```
[記事一覧] news/index.html ──fetch──▶ data/news.json （status!='draft' のみ表示）
[記事保存] news/admin.html ──POST──▶ api/save.js ──GitHub API──▶ data/news.json を直接コミット
                                                              └▶ push を検知して Actions がSNS素材生成/X投稿
```

---

## 3. 管理画面（CMS）

- URL: https://actorsbookmag.vercel.app/news/admin.html
- ログイン: Vercel環境変数 **`EDIT_PASSWORD`** の値を入力
- 保存の仕組み: ブラウザ → `api/save.js`（Vercel Function）→ サーバー側に隠した **`GITHUB_TOKEN`** で `data/news.json`・画像を GitHub に直接コミット → Vercel が自動再デプロイ（反映まで約1分）
- 記事の公開/非公開は `status`（`draft` = 非公開 / それ以外 = 公開）で制御

> CMSはローカルの `git` を経由せず **GitHub へ直接コミット**する。そのためローカルのクローンはすぐに古くなる（§6 参照）。

---

## 4. 自動生成パイプライン（scripts/pipeline.py）

ローカルで手動実行してオリジナル記事を生成する。仮想環境と `.env` が必要。

```bash
# ネタ候補を番号付きで一覧（API課金なし）
./.venv/bin/python scripts/pipeline.py ideas --limit 10

# 選んだネタ＋ひとこと指示で記事＋サムネを生成
./.venv/bin/python scripts/pipeline.py make --pick 1 --direction "エッジ効かせて"

# シーンのお題からオリジナル記事を書く
./.venv/bin/python scripts/pipeline.py make --topic "自主配給の現在地" --brief "確認済みの事実メモ"
```

- 記事生成には **Claude API**（`ANTHROPIC_API_KEY`）を使用。キーはリポジトリ直下の `.env`（gitignore済）に置く。
- 生成した記事は `data/news.json` に追記され、サムネは `images/news/` に出力される。

---

## 5. GitHub Actions（自動化）

**⚠ これらのワークフローとパイプラインは壊さないこと。**

| ワークフロー | トリガー | 処理 | 使う環境変数(Secrets) |
|---|---|---|---|
| `fetch_films.yml` | 毎朝 **06:17 JST**（cron `17 21 * * *` UTC）＋手動 | `fetch_films.py` で配信新着取得 → `generate_banners.py` → `data/films.json`・`data/banners/` をコミット | `ANTHROPIC_API_KEY` |
| `make_assets.yml` | `data/news.json` への push＋手動 | `make_assets.py --all-published` でSNS素材生成 → `social/` をコミット | （なし） |
| `post_x.yml` | `data/news.json` への push＋手動 | `post_x.py` で公開・未投稿記事をXへ投稿 | `X_API_KEY` `X_API_SECRET` `X_ACCESS_TOKEN` `X_ACCESS_SECRET` |

- いずれも `github-actions[bot]` としてコミットし、`[skip ci]` で無限ループを防いでいる。
- Actions のコミットはリモートに直接入る。ローカルは定期的に `git pull` して追従する（§6）。

> **注意（2026-08 時点）**: `post_x.yml` と `scripts/post_x.py` によるX投稿機能、および `news/admin.html` の「公開する（＋X投稿画面）」ボタンは**作業途中（ローカル未コミット/未追跡）**の状態がある。X投稿を本番稼働させる際は、これらをコミット＆push し、Xの4キーを GitHub Secrets に登録してから有効化すること。

---

## 6. 環境変数

### Vercel（プロジェクト設定 → Environment Variables）
| 変数 | 用途 |
|---|---|
| `EDIT_PASSWORD` | CMS(`admin.html`)のログインパスワード |
| `GITHUB_TOKEN` | `api/save.js` が `news.json`・画像を GitHub にコミットするためのトークン（`contents:write` 権限） |

### GitHub（Settings → Secrets and variables → Actions）
| 変数 | 用途 |
|---|---|
| `ANTHROPIC_API_KEY` | Claude API（記事生成・映画取得） |
| `X_API_KEY` / `X_API_SECRET` / `X_ACCESS_TOKEN` / `X_ACCESS_SECRET` | X自動投稿（`post_x.yml`） |

### ローカル（`.env`、gitignore済）
| 変数 | 用途 |
|---|---|
| `ANTHROPIC_API_KEY` | `pipeline.py` 実行用 |
| （Xキー4つ） | `post_x.py` を手動実行する場合のみ |

---

## 7. 開発・デプロイの流れ

1. `git pull`（**必ず最初に**。CMS・Actionsがリモートを先に進めているため）
2. ローカルで編集
3. `git add <対象ファイルのみ>` → `git commit` → `git push`
4. Vercel が `main` への push を検知して自動デプロイ（数十秒〜1分）

> `git add -A` は避ける。作業途中のファイル（`admin.html` のWIP、`post_x.yml`、`.DS_Store` 等）を巻き込まないよう、**コミット対象は明示的に指定**する。

---

## 8. 復旧手順（トラブルシューティング）

### `/` を旧TOPに戻したい / リダイレクトを外したい
`vercel.json` の `redirects` エントリを削除して push。`/` で `index.html` が再表示される。

### ローカルがリモートと乖離した（push が rejected される）
CMS・Actionsがリモートを進めているのが原因。原則ローカル側は追従するだけでよい。
```bash
git fetch origin
git log --oneline --left-right origin/main...main   # どちらが進んでいるか確認
# リモートだけ進んでいる（ローカル変更なし）なら：
git merge --ff-only origin/main
```
ローカルに未コミット変更があってff-onlyが止まる場合は、対象ファイルを `git stash` → `merge --ff-only` → `git stash pop`。

### CMSで保存できない（`admin.html`）
- 「パスワードが違います」→ Vercelの `EDIT_PASSWORD` を確認
- 「サーバー未設定: GITHUB_TOKEN がありません」→ Vercelに `GITHUB_TOKEN` を登録／期限切れなら再発行
- 「保存に失敗 …」→ GitHub API側のエラー。トークンの権限（`contents:write`）とレート制限を確認

### GitHub Actions が赤（fetch_films）
スクレイピング先の構造変化が主因。Actionsログを確認し、`scripts/fetch_films.py` の対象セレクタ・待機時間を調整。映画データが古いままでも本番サイト（記事）は影響を受けない。

### デプロイが反映されない
Vercelダッシュボードで該当デプロイのステータス／ビルドログを確認。`vercel.json` の記法エラーはビルド失敗の原因になるため、変更時はJSONの妥当性に注意。

---

## 9. 今後の方向性（メモ）

- `/news` のデザインを **Hypebeast 系の洗練エディトリアル路線**（黒基調・極太タイポ・大きな写真・グリッド）へ寄せる。
- 旧FRAME系ページは段階的に整理する。
