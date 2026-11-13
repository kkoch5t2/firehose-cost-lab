# Amazon Data Firehose Direct PUT料金の検証

小さなログを1行ずつFirehoseへ渡す場合と、送信側で複数行を連結して1レコードにする場合を比較する検証用コードです。CloudFormationテンプレートは、今回の検証に使用したものです。

## 構成

| リソース | 用途・設定 |
|---|---|
| Amazon Data Firehose × 2 | Direct PUT → Amazon S3。A・Bの比較用 |
| Amazon S3 × 1 | Aは`a/`、Bは`b/`へ保存。公開アクセスをブロックし、SSE-S3で暗号化 |
| IAMロール × 1 | Firehoseから作成したS3バケットへのアクセスを許可 |

両ストリームの配信バッファは60秒・5 MiB、圧縮なしです。EC2、Lambda、Kinesis Data Streamsは作成しません。Firehoseの配信エラーログは無効にしています。

**S3オブジェクトは7日後に期限切れとなるライフサイクル設定です。** 記事や監査に必要な原本・実測結果・画面キャプチャは別途保存してください。CloudFormationを作成してもデータ送信は自動実行されません。

## ファイル

- `template.json`：CloudFormationテンプレート。
- `lab.py`：有限の架空ログ送信とS3到着確認。
- `requirements.txt`：Python依存パッケージ。

実アカウントID、認証情報、実ログ、検証結果のmanifest、画面キャプチャは含めていません。認証には利用者自身のAWSプロファイルを使います。

## 前提

Git、AWS CLI v2、Pythonと、CloudFormation・Firehose・S3・IAMを操作できるAWS認証プロファイルが必要です。以下はWindows PowerShellの例です。プロファイル名は自分の環境に置き換えてください。

```powershell
git clone https://github.com/kkoch5t2/firehose-cost-lab.git
cd firehose-cost-lab
$env:AWS_PROFILE = "firehose-lab"
$env:AWS_DEFAULT_REGION = "ap-southeast-2"
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

`requirements.txt`には検証環境で使用した`boto3[crt]==1.44.1`を記載しています。`aws login`の認証情報をBoto3で利用する場合に必要なCRTも含みます。

## AWSへ接続せずに計算を確認する

```powershell
# A：200 Bを100万レコードとして扱う場合
.\.venv\Scripts\python.exe lab.py --scenario a --kind bulk --lines 1000000 --dry-run
# B：60行を1レコードに連結する場合
.\.venv\Scripts\python.exe lab.py --scenario b --kind bulk --lines 1000000 --dry-run
```

`--dry-run`ではAWS APIを呼びません。料金の計算値はコード内の参考単価に基づき、請求実績ではありません。単価はリージョン・送信先・料金区分で変わるため、利用時に公式料金を確認してください。

## 環境を作成する

次のコマンドはAWSリソースを作成します。データ送信・S3保存・読み取りなどには料金が発生します。

```powershell
aws cloudformation deploy --stack-name firehose-cost-lab --template-file template.json --capabilities CAPABILITY_IAM
```

スタックの出力から`lab.py`がバケット名とストリーム名を取得します。別のスタック名を使う場合は、送信コマンドにも`--stack <スタック名>`を付けてください。

## 60秒間の動作を確認する

```powershell
# A：毎秒1行を生成して、その都度送信する
.\.venv\Scripts\python.exe lab.py --scenario a --kind live --run-id livea001
# B：毎秒1行を60秒間ためて、60行を1レコードとして送信する
.\.venv\Scripts\python.exe lab.py --scenario b --kind live --run-id liveb001
```

各方式の元データは60行、改行込み200 B/行です。Bでは60秒分をためた結果が60行になります。この送信側バッファと、FirehoseからS3へ配信する際の60秒・5 MiBのバッファは別です。

送信完了後、S3への配信を待ってから、保存されたmanifestを使って到着行数を照合します。

```powershell
.\.venv\Scripts\python.exe lab.py --scenario a --verify result-livea001-a.json
.\.venv\Scripts\python.exe lab.py --scenario b --verify result-liveb001-b.json
```

## 100万行で料金を比較する

以下は追加の送信費用が発生するコマンドです。必要な場合だけ実行してください。方式ごとの請求を分けるには、専用環境と異なるUTC日付などを使って他の送信と区別します。

```powershell
.\.venv\Scripts\python.exe lab.py --scenario a --kind bulk --lines 1000000 --run-id bulka001
.\.venv\Scripts\python.exe lab.py --scenario b --kind bulk --lines 1000000 --run-id bulkb001
```

両方式とも`PutRecordBatch`を使います。Bは60行単位で連結し、**各グループを作るたびの60秒待機は省略**します。100万行の条件は各方式200,000,000 Bで、Aが1,000,000レコード、Bが16,667レコードです。最後のBレコードは40行です。

取り込み料金の比較対象は、通常のDirect PUT → S3におけるレコードごとの5 KiB切り上げです。API呼び出しをバッチ化するだけでは、各レコードの切り上げを避けられません。送信時間の比較を目的としたコードではありません。

## 再実行・保存時の注意

同じrun IDで送信を繰り返すと二重送信になります。永続ロックや送信キューの永続化はありません。到着待ちでは送信をやり直さず、`--verify`のみを再実行してください。SDK再試行などによる重複の可能性もあるため、行の連番で確認します。

このコードは検証用であり、本番用ログ収集エージェントではありません。自動削除・後片付け処理は含みません。原本、設定画面、照合結果、請求情報を保存・確認してから、利用者の判断で環境を管理してください。

## 公式資料

- [Amazon Data Firehoseの料金](https://aws.amazon.com/firehose/pricing/)
- [Amazon Data Firehose FAQ](https://aws.amazon.com/firehose/faqs/)
- [BufferingHints](https://docs.aws.amazon.com/firehose/latest/APIReference/API_BufferingHints.html)
