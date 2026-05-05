# CHUPPY RAG CONVERTER (ondemand) — バイブコーディング用プロンプト

この Markdown は、AI コーディング支援ツールにそのまま渡すための実装プロンプトです。  
Flask ベースのローカルネットワーク向けブラウザアプリ **CHUPPY RAG CONVERTER** を、要件ぶれなく実装・修正させることを目的としています。

---

## 使い方

- このファイルをそのまま AI に渡してください
- 最新版 ZIP（`ondemand_only_project.zip`）も一緒にアップロードすると実装精度が安定します
- 既存コードを修正させる場合も、新規作成と同じ指示で使えます

---

## 実装 AI への依頼文

あなたは、製造業向け社内業務アプリを実装するシニアフルスタックエンジニアです。  
以下の仕様を満たす Flask アプリケーションを実装・修正してください。

### ゴール

社内ネットワーク上の共有フォルダ（UNC パス）にアップロードされたファイルを、  
**Dify Chat API** を通じて **RAG 向け Markdown** へ自動変換・保存する ondemand 専用アプリ **CHUPPY RAG CONVERTER** を実装すること。

---

## 非交渉の実装ルール

以下は必須です。解釈で省略しないでください。

1. バックエンドは **Python + Flask** を使用すること
2. アプリは **同一ネットワーク上の端末からアクセス可能** であること
3. Flask 起動ホストは `0.0.0.0`、ポートは **5220** に固定すること
4. アプリケーションタイトルは **CHUPPY RAG CONVERTER** にすること
5. HTML / CSS / JavaScript は分離し、**CSS と JS は外部ファイル化** すること
6. `.env` から API キー等の設定を読み込むこと（`python-dotenv` 使用）
7. Windows 環境でもタイムゾーン取得で落ちないよう、**`tzdata` を依存関係に含める** こと
8. Windows UNC パス（`\\server\share\...`）を正しく扱うこと
9. コードに `TODO`、未実装プレースホルダ、説明用ダミー処理を残さないこと
10. 不要なコメントアウトを入れないこと
11. CDN や外部 SaaS に依存しないこと
12. 出力は **ファイル単位** で、保存可能な完全な内容にすること
13. 既存ファイルを修正する場合も **差分ではなく全文** を出力すること
14. このファイル（`CLAUDE.md`）を変更した場合も必ず全文を出力すること

---

## フォルダ構成

```text
ondemand_only_project/
├─ app.py
├─ ondemand_core.py
├─ ondemand_queue.py
├─ prompt.md
├─ templates/
│  └─ ondemand.html
└─ static/
   ├─ ondemand.js
   └─ style.css
```

---

## 環境変数（.env）

| 変数名 | 用途 |
|---|---|
| `DIFY_API_BASE` | Dify Chat API のベース URL |
| `DIFY_API_KEY` | Dify Chat API キー |
| `CHUPPY_EXPLORER_ROOT` | 監視対象ルートフォルダ（UNC パス可）。未設定時は `\\172.27.23.54\disk1\Chuppy` |
| `ONDEMAND_MONITOR_INTERVAL_SEC` | フォルダ監視間隔（秒）。最低 3 秒、デフォルト 15 秒 |
| `ONDEMAND_QUEUE_MAX_RETRIES` | キュー最大リトライ回数。デフォルト 5 |

---

## フォルダ階層仕様

```
Lv1  : 1文字（例: P）
Lv2  : 2文字（例: PJ）
Lv3  : 任意（例: PJ4）
Lv4  : 固定名 — 「元データ」または「マークダウン形式」
Lv5  : カテゴリフォルダ（アップロード・閲覧対象）
```

- **`UPLOAD_ALLOWED_DEPTH = 5`**：Lv5 フォルダのみアップロード可
- **`EXPLORER_MAX_DEPTH = 5`**：ツリー表示はここまで
- Lv4「元データ」は **UI ツリーに表示しない**。見た目上は Lv3 → Lv5 を選ぶ形にする

### Lv5 カテゴリ一覧と絵文字

| カテゴリ名 | 絵文字 |
|---|---|
| 安全 | 💞 |
| 品質 | 🏅 |
| 生産 | 🏭 |
| 保全 | 🪛 |
| 環境 | 🌳 |
| 原価 | 💰 |
| 人材育成 | 🏫 |
| 安全健康 | 💞 |
| 専門技能 | 💪 |
| TPS | 📊 |
| 人事 | 👤 |
| 総務 | 🧑‍💻 |

---

## ファイル命名ルール

### アップロード保存名

アップロード時に以下のタイムスタンププレフィックスを付与する。

```
yyyymmdd_HHMMSS_元ファイル名.拡張子
例: 20260105_215015_report.pdf
```

同名ファイルが既に存在する場合はサフィックスで一意化する（`_2`, `_3` …）。

### Markdown 保存先ルール

| 項目 | 内容 |
|---|---|
| 保存先フォルダ | 元データと同じ Lv3 配下の「**マークダウン形式**」→「**同一 Lv5 フォルダ名**」 |
| ファイル名 | アップロード保存名の **拡張子を `.md` に変えたもの** |
| 差分判定 | 保存先に同名 `.md` ファイルが存在する場合はスキップ（完全一致） |

**例:**

```
元データ  : \\172.27.23.54\disk1\Chuppy\P\PJ\PJ4\元データ\専門技能\20260105_215015_report.pdf
Markdown : \\172.27.23.54\disk1\Chuppy\P\PJ\PJ4\マークダウン形式\専門技能\20260105_215015_report.md
```

---

## 対応拡張子

```
.txt .md .csv .json .log .html .xml .yml .yaml .ini .conf
.py .js .css
.docx .pdf
.xlsx .xls .xlsm
.ppt .pptx
```

---

## アップロード制約

| 制約 | 内容 |
|---|---|
| 非対応拡張子 | 内容一覧で非表示 / アップロード不可 / キュー追加不可 |
| ファイルサイズ | 1 ファイル **100MB** を超えたらフロント・サーバー両方で拒否 |
| 一括上限 | 一度に最大 **100 ファイル** |
| 対象深度 | Lv5 フォルダのみ受付 |

---

## キュー仕様

- **フォルダ公平性を保つラウンドロビン方式**
- 最大リトライ回数: `ONDEMAND_QUEUE_MAX_RETRIES`（デフォルト 5 回）
- リトライ超過で最終エラー → **元ファイル・Markdown を自動削除**
- キュー完了履歴の上限: 300 件（古いものから削除）
- ソース署名（ファイルパス＋サイズ等）で重複投入を防ぐ

### キュータスクの状態

| 状態 | 説明 |
|---|---|
| `queued` | 投入済み・待機中 |
| `running` | 変換処理中 |
| `completed` | 変換完了 |
| `skipped` | Markdown が既に存在するためスキップ |
| `error` | エラー（リトライ残あり or 最終エラー） |

---

## 削除仕様

### 内容一覧からの削除

1. 元ファイルを削除
2. 対応する Markdown ファイルを削除（完全一致パス）
3. キュー上の同一ソースのタスクを除去

### キューエラー行の手動削除

- `POST /api/ondemand/queue/<task_id>/delete`
- 元ファイル + Markdown + キューエントリを削除

### キューエラー行の手動再試行

- `POST /api/ondemand/queue/<task_id>/retry`
- Markdown のみ削除し、リトライカウントをリセットしてキュー最後尾へ再投入

---

## フォルダ監視

- `app.py` 起動時からバックグラウンドスレッドで常時監視
- 監視間隔: `ONDEMAND_MONITOR_INTERVAL_SEC`（デフォルト 15 秒）
- 差分検出: 「元データ」Lv5 フォルダ内ファイルに対応する Markdown が存在しない場合にキュー投入
- 差分判定は完全一致パスで行う（パターン検索なし）

---

## Markdown 変換仕様

- **Dify Chat API** を使用（`DIFY_API_BASE` / `DIFY_API_KEY`）
- 変換スタイル: **RAG 向け Markdown**（`prompt.md` に変換指示を記載）
- chunk separator: `***`
- API タイムアウト: 300 秒
- Dify ナレッジへの登録は行わない（Markdown ファイル保存のみで完了）

---

## ログ仕様

ファイル: `ondemand_queue.log`（プロジェクト直下）  
形式: タブ区切り 4 列

```
timestamp	folder_name	file_name	status
```

---

## API エンドポイント一覧

| メソッド | パス | 用途 |
|---|---|---|
| GET | `/` または `/ondemand` | ondemand 画面返却 |
| GET | `/api/datasets` | 常に空リスト返却（Dify ナレッジ登録なし） |
| GET | `/api/explorer/root` | ルートフォルダ情報 |
| GET | `/api/explorer/list` | フォルダ内容一覧（ファイル状態付き） |
| POST | `/api/explorer/upload` | ファイルアップロード＋キュー投入 |
| POST | `/api/explorer/delete` | ファイル削除（元ファイル＋Markdown＋キュー） |
| GET | `/api/ondemand/queue` | キュースナップショット取得 |
| POST | `/api/ondemand/queue/<task_id>/delete` | キューエラー行の手動削除 |
| POST | `/api/ondemand/queue/<task_id>/retry` | キューエラー行の手動再試行 |

---

## 各ファイルの役割

### `app.py`

- Flask ルート定義
- アプリ起動（`host="0.0.0.0"`, `port=5222`）
- `OnDemandQueueManager` / `OnDemandFolderMonitor` のインスタンス生成と起動

### `ondemand_core.py`

- 共通設定（定数・環境変数読み込み）
- ファイルテキスト抽出（`extract_text`）
- パス解決・フォルダ探索・深度計算
- Markdown パス生成（`build_ondemand_markdown_path`）
- 削除処理（`delete_ondemand_artifacts` / `cleanup_markdown_only`）
- ファイル状態判定（`resolve_explorer_file_registration_status`）
- Dify Chat API 呼び出し（`convert_via_dify_chat_messages_secure`）
- キャッシュ管理
- ログ出力（`append_ondemand_queue_log`）

### `ondemand_queue.py`

- `OnDemandQueueManager`: フェアラウンドロビンキュー制御・リトライ・自動削除・エラー手動操作
- `OnDemandFolderMonitor`: 定期フォルダ監視・差分検出・キュー投入

### `templates/ondemand.html`

- 画面の骨組み（ヘッダー / サイドバー / ドロップゾーン / タブ / 内容一覧 / キュー）

### `static/ondemand.js`

- ツリー描画・展開操作
- API 通信（一覧取得・アップロード・削除・キュー操作）
- 内容一覧のソート・状態表示・定期更新
- タブ切替
- キューエラー行の手動削除・再試行操作

### `static/style.css`

- 固定ヘッダー・左固定サイドバーレイアウト
- コンパクトなツリー表示
- テーブル・状態ラベル・ボタンのスタイル

---

## UI 仕様

### 全体レイアウト

- **固定ヘッダー** + **左固定サイドバー（フォルダツリー）** + **メインコンテンツ**
- 画面は ondemand.html のみ

### フォルダツリー

- Lv1 が初期展開状態
- Lv4「元データ」は表示しない（Lv3 から直接 Lv5 に見える）
- 展開記号: 展開前 `>` / 展開後 `∨`
- フォルダ名テキストをクリックしても展開・閉じる
- ツリー右側に配下対応ファイル数を `人材育成(30)` 形式で**赤文字**表示
- Lv5 フォルダ名の前に上記カテゴリ絵文字を付与
- コンパクトなレイアウト

### メインコンテンツ — タブ切替

**タブ 1: 内容一覧**

- 選択した Lv5 フォルダ内のファイル一覧を表示
- 非対応拡張子ファイルは非表示
- 列: ファイル名（拡張子アイコン付き） / 状態 / 更新日時 / サイズ
- 状態ラベル: `変換済` / `未変換` / `エラー` / `変換中`
- 状態は定期更新で常に最新を追従
- 各列で昇順・降順ソート可能（デフォルト: 更新日時降順）
- 各行に削除ボタン

**タブ 2: アップロード待ちキュー**

- キューの全タスクを表示（完了は折りたたみ等でコンパクトに）
- ファイル名左に拡張子アイコン表示
- エラー行にのみ **再試行** / **削除** ボタンを表示

### ドロップゾーン

- コンパクト表示
- 対応拡張子一覧を表示
- 1 ファイル最大 100MB の注意書きを表示
- ドラッグ&ドロップ + クリックで選択の両方に対応

---

## 出力形式

最終出力は以下の順で、**ファイルごとに完全な内容** を提示してください。

1. `app.py`
2. `ondemand_core.py`
3. `ondemand_queue.py`
4. `templates/ondemand.html`
5. `static/ondemand.js`
6. `static/style.css`
7. `CLAUDE.md`（変更した場合）

出力ルール:

- 各ファイルは保存可能な完全な内容にすること
- 省略記法（`...` や「以下同様」）を使わないこと
- 差分ではなく全文を出すこと
- 途中で説明だけに逃げず、必ずファイル本体を出すこと
