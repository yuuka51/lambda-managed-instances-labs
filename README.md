# lambda-managed-instances-lab

このリポジトリは、**AWS Lambda / Lambda Managed Instances / EC2** で同一ワークロードを再現し、
TSV整合性検証パイプラインの挙動を比較するための **検証用リポジトリ** です。

## 目的

- `src/lmi_lab/impls/codex/engines/` の実装をベースに検証
- strictness A/B を共通仕様で比較
- 8エンジン（duckdb / duckdb-opt / pandas / pandas-opt / polars / polars-opt / fireducks / spark）で同一ワークロードを実行
- AWS実験（S3→/tmp→S3）を `aws_runner` / `lambda_handler` で再現
- 参照実装（reference）は小規模データの correctness oracle として隔離

## ディレクトリ構成

```text
src/lmi_lab/
  __init__.py
  __main__.py
  cli.py
  lambda_handler.py
  core/
    __init__.py
    config.py
    schema.py
    normalize.py
    io_s3.py
    metrics.py
    result_writer.py
  runners/
    __init__.py
    compare.py
    benchmark.py
    aws_runner.py
  impls/codex/engines/
    __init__.py
    common.py
    duckdb_engine.py
    duckdb_engine_optimized.py
    pandas_engine.py
    pandas_engine_optimized.py
    polars_engine.py
    polars_engine_optimized.py
    fireducks_engine.py
    spark_engine.py
  reference_impl/
    __init__.py
    reference_engine.py
  data_prep/
    __init__.py
    generate.py
    upload_s3.py
infra/
  cfn/
    ec2-s3-ssm.yml
  events/
    stage1_tmp.json
    stage2_mem.json
    stage3_time.json
scripts/
  make_dataset.sh
  setup_venv.sh
tests/
  test_normalize.py
  test_reference_oracle.py
  test_codex_common.py
  test_codex_common_primary_keys.py
  test_data_prep_generate.py
  test_duckdb_engine.py
  test_fireducks_engine.py
  test_pandas_engine.py
  test_polars_engine.py
  test_polars_null_handling.py
  test_spark_engine.py
  test_aws_runner_artifacts.py
  test_aws_runner_memray.py
  fixtures/
    before_small.tsv
    after_small.tsv
```

## strictness仕様

対象カラム: `user_id, tags, levels, timestamps, op_type`

- **A（互換）**
  - `tags/levels/timestamps`: `split('|') -> strip -> sort(文字列) -> join('|')`
  - `timestamps` も文字列扱い（フォーマット変換しない）
  - NULL/空白は `""` に統一
- **B（厳密寄り）**
  - `tags/levels`: int化可能なら int として扱い、ソートキー `(type_order, value)`
  - `timestamps`: 文字列として順不同正規化のみ（フォーマット変換しない）
  - `op_type`: `I` は `U` とみなし、`D` は比較対象から除外

## エンジン一覧

| エンジン名 | 実装 | 説明 |
|-----------|------|------|
| `duckdb` | 標準 | DuckDB で読み込み → Python辞書変換 → 共通diff |
| `duckdb-opt` | 最適化 | SQL内でFULL OUTER JOIN完結。Python変換を排除 |
| `pandas` | 標準 | Pandas で読み込み（ワイルドカード対応）→ Python辞書変換 → 共通diff |
| `pandas-opt` | 最適化 | チャンク処理 + ハッシュベース比較（※`iterrows()`使用のため低速） |
| `polars` | 標準 | Polars で読み込み → Python辞書変換 → 共通diff |
| `polars-opt` | 最適化 | Lazy評価 + anti_join/inner_join で差分検出 |
| `fireducks` | 標準 | FireDucks（Pandas互換API）で読み込み → Python辞書変換 → 共通diff |
| `spark` | 標準 | PySpark local mode で読み込み → Python辞書変換 → 共通diff |

> `reference` エンジンは小規模専用の correctness oracle です。`--engines all` には含まれません。

## ローカル実行

```bash
# セットアップ
./scripts/setup_venv.sh
source .venv/bin/activate

# データ生成
python -m lmi_lab generate --outdir data/run-small --rows 1000 --num-files 2 --gzip false

# 単一エンジンで比較
python -m lmi_lab compare \
  --before data/run-small/before/part-00000.tsv \
  --after data/run-small/after/part-00000.tsv \
  --outdir out \
  --engines duckdb --strictness A

# 複数エンジンでベンチマーク
python -m lmi_lab benchmark \
  --before data/run-small/before/part-00000.tsv \
  --after data/run-small/after/part-00000.tsv \
  --outdir out \
  --engines duckdb,polars,pandas --strictness A

# 最適化エンジンでベンチマーク
python -m lmi_lab benchmark \
  --before data/run-small/before/part-00000.tsv \
  --after data/run-small/after/part-00000.tsv \
  --outdir out \
  --engines duckdb-opt,polars-opt --strictness A
```

## AWS実行

`src/lmi_lab/lambda_handler.py` は `runners/aws_runner.py` を呼び出します。
イベントJSON例は `infra/events/` を使用してください。

- `stage1_tmp.json`: `/tmp` 制約
- `stage2_mem.json`: strictness B + memray
- `stage3_time.json`: spark含む時間検証

S3入出力:
1. `before/after` を S3 から `/tmp` へダウンロード
2. ローカルと同じ compare/benchmark 実行
3. `diff/benchmark/summary` を S3へアップロード

## CloudFormationで再現環境を作成

`infra/cfn/ec2-s3-ssm.yml` で VPC / S3 / EC2 / SSM / VPCエンドポイント / IAM を一括作成できます。
VPCエンドポイント（SSM / EC2Messages / SSMMessages / S3）を含むプライベートサブネット構成で、NAT Gatewayなしで動作します。

### S3アーティファクト方式（Private リポジトリ対応）

このテンプレートは、GitHubトークンを使わずにPrivateリポジトリを運用できるよう、
S3からZIPファイルを取得して展開する方式を採用しています。

#### 1. リポジトリZIPの作成

リポジトリのルートディレクトリで以下を実行:

```bash
zip -r lambda-managed-instances-lab.zip . \
  -x ".git/*" "*__pycache__/*" ".venv/*" ".pytest_cache/*" "*.egg-info/*" ".DS_Store"
```

このコマンドは、カレントディレクトリ (`.`) の内容を直接ZIPに含めます。
展開時に `pyproject.toml` や `src/` がZIPのルートに配置されます。

> 注: GitHub の "Download ZIP" 機能を使うと、トップディレクトリ付きのZIPになります。
> その場合でも、UserDataが自動的に検出して対応します。

#### 2. S3へのアップロード

アーティファクト用のS3バケットにZIPをアップロード:

```bash
aws s3 cp lambda-managed-instances-lab.zip s3://<ArtifactBucket>/artifacts/lmi-lab.zip
```

> `<ArtifactBucket>` は既存のバケット、または新規作成したバケット名に置き換えてください。

#### 3. CloudFormationスタックの作成

```bash
aws cloudformation deploy \
  --template-file infra/cfn/ec2-s3-ssm.yml \
  --stack-name lmi-lab \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ArtifactBucket=<your-artifact-bucket> \
    ArtifactKey=artifacts/lmi-lab.zip \
    AvailabilityZone=ap-northeast-1a \
    DatasetPrefix=inputs/run-real/ \
    ResultsPrefix=results/
```

#### パラメータ説明

| パラメータ | 説明 | デフォルト |
|-----------|------|-----------|
| `ArtifactBucket` | リポジトリZIPを配置したS3バケット名 | （必須） |
| `ArtifactKey` | ZIPファイルのS3オブジェクトキー | `artifacts/lmi-lab.zip` |
| `VpcCidr` | VPCのCIDRブロック | `192.168.0.0/16` |
| `PrivateSubnetCidr` | プライベートサブネットのCIDRブロック | `192.168.20.0/24` |
| `AvailabilityZone` | サブネットのアベイラビリティゾーン | `ap-northeast-1a` |
| `InstanceType` | EC2インスタンスタイプ | `r7i.xlarge` |
| `RootVolumeGiB` | ルートボリュームサイズ（GB） | `512` |
| `DatasetPrefix` | データセット用のS3プレフィックス | `inputs/run-real/` |
| `ResultsPrefix` | 結果出力用のS3プレフィックス | `results/` |
| `BucketName` | データバケット名（省略時は自動生成） | （空欄） |

作成後、Outputs の `SSMConnectionCommand` を使って接続できます。

## データ生成とS3配置

- 一括スクリプト: `scripts/make_dataset.sh`

```bash
# 例: 小規模データを生成し、BUCKETがあればアップロード
BUCKET=<your-bucket> scripts/make_dataset.sh
```

## Spark対応範囲

現状はまず再現性重視のため、Sparkは読み込みをSparkで行い、
正規化は共通Python関数を使用しています（strictness A/B とも同じ仕様で動作）。

## テスト

```bash
# 全テスト実行
pytest tests/

# 正規化ロジックのみ
pytest tests/test_normalize.py

# correctness oracle（reference vs エンジン）
pytest tests/test_reference_oracle.py
```

CIでは小規模データのみを対象に correctness を確認します。

## 注意事項

- `metrics.py` の `peak_rss_mb()` は `resource.getrusage` を使用しているため、Linux / macOS 環境でのみ動作します。
- `data_prep/generate.py` は外部ライブラリに依存せず、自前の疑似乱数（splitmix64）で再現性を保証しています。
- 最適化実装（`*-opt`）は正規化ロジックを簡略化しています。標準実装と結果が異なる場合があります。
