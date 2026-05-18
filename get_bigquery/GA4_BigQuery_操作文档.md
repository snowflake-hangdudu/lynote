# GA4 BigQuery 数据拉取操作文档

> 适用项目：`lynote` / 数据集：`analytics_512358614`

---

## 一、管理员操作（一次性配置）

> 需要 `lynote` GCP 项目的 Owner 或 IAM 管理员权限

### 1. 开启 GA4 → BigQuery 导出

1. 进入 [GA4 管理后台](https://analytics.google.com) → **Admin** → **BigQuery Links**
2. 点击 **Link**，选择 GCP 项目 `lynote`
3. Export type 选择 `Daily`，Region 选就近区域
4. 点击 **Submit**

> ⚠️ 导出从当天起生效，历史数据不补充

### 2. 给成员账号授权

1. 打开 [GCP IAM 控制台](https://console.cloud.google.com/iam-admin/iam)，选择项目 `lynote`
2. 点击 **Grant Access**
3. 填入成员的 Google 账号邮箱
4. 添加以下两个角色：
   - `BigQuery Data Viewer`
   - `BigQuery Job User`
5. 点击 **Save**

---

## 二、成员本地环境配置（一次性）

### 1. 安装 Python 依赖

```bash
pip install google-cloud-bigquery pandas pyarrow pytz db-dtypes
```

### 2. 安装 Google Cloud CLI

前往 [cloud.google.com/sdk/docs/install](https://cloud.google.com/sdk/docs/install) 下载安装

- Windows：下载 `GoogleCloudSDKInstaller.exe`，一路默认安装
- Mac/Linux：按页面说明安装

安装完成后**重启终端**（或重启 VS Code）。

### 3. 登录认证

在终端执行：

```bash
gcloud auth application-default login --scopes=https://www.googleapis.com/auth/cloud-platform
```

浏览器会弹出 Google 登录页，使用管理员已授权的账号登录，点击允许。

出现以下提示即为成功：

```
Credentials saved to file: [.../application_default_credentials.json]
```

> ✅ 认证只需做一次，凭证会自动保存在本地

---

## 三、运行数据拉取脚本

### 1. 准备文件

将 `fetch_ga4.py` 放到任意目录，例如：

```
D:\lynote\get_bigquery\
└── fetch_ga4.py
```

### 2. 运行

```bash
python fetch_ga4.py
```

### 3. 输出文件

运行成功后，同目录下会生成：

| 文件 | 内容 |
|------|------|
| `ga4_event_summary.csv` | 近 7 天所有事件汇总（按事件名 + 日期统计） |
| `ga4_cancel_sub_detail.csv` | 近 7 天取消订阅事件明细（含用户 ID、设备、国家等） |

---

## 四、fetch_ga4.py 完整代码

```python
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz
import pandas as pd

# ── 配置 ──────────────────────────────────────────────
BQ_PROJECT_ID = "lynote"
BQ_DATASET    = "analytics_512358614"
GA4_TIMEZONE  = "Asia/Shanghai"

# ── 初始化客户端（使用 gcloud 本地认证） ──────────────────
client = bigquery.Client(project=BQ_PROJECT_ID)

# ── 日期工具 ───────────────────────────────────────────
def get_date_range(days_back=7):
    tz = pytz.timezone(GA4_TIMEZONE)
    today = datetime.now(tz).date()
    start = today - timedelta(days=days_back)
    return start.strftime("%Y%m%d"), today.strftime("%Y%m%d")

def get_today_str():
    tz = pytz.timezone(GA4_TIMEZONE)
    return datetime.now(tz).strftime("%Y%m%d")

# ── 通用查询 ───────────────────────────────────────────
def query(sql: str) -> pd.DataFrame:
    print("🔍 查询中...")
    result = client.query(sql).result()
    df = result.to_dataframe()
    print(f"✅ 返回 {len(df)} 行\n")
    return df

# ── 查询1：事件汇总 ────────────────────────────────────
def fetch_event_summary(days_back=7) -> pd.DataFrame:
    start, _ = get_date_range(days_back)
    today = get_today_str()
    sql = f"""
    SELECT
        event_date,
        event_name,
        COUNT(*)                       AS event_count,
        COUNT(DISTINCT user_pseudo_id) AS unique_users
    FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start}' AND 'intraday_{today}'
    GROUP BY event_date, event_name
    ORDER BY event_date DESC, event_count DESC
    """
    return query(sql)

# ── 查询2：原始事件明细 ────────────────────────────────
def fetch_event_detail(days_back=7, event_names: list = None) -> pd.DataFrame:
    start, _ = get_date_range(days_back)
    today = get_today_str()
    event_filter = ""
    if event_names:
        names = ", ".join(f"'{e}'" for e in event_names)
        event_filter = f"AND event_name IN ({names})"

    sql = f"""
    SELECT
        event_date,
        FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',
            TIMESTAMP_MICROS(event_timestamp), '{GA4_TIMEZONE}') AS event_time,
        event_name,
        user_pseudo_id,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location')  AS page_url,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title')     AS page_title,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_id')     AS session_id,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'lynote_user_id') AS lynote_user_id,
        device.category                                                                     AS device_type,
        geo.country                                                                         AS country
    FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{start}' AND 'intraday_{today}'
    {event_filter}
    LIMIT 50000
    """
    return query(sql)

# ── 导出 CSV ──────────────────────────────────────────
def export_csv(df: pd.DataFrame, filename: str):
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"📁 已导出：{filename}（{len(df)} 行）\n")

# ── 主流程 ─────────────────────────────────────────────
if __name__ == "__main__":
    # 1. 事件汇总（近 7 天）
    print("=" * 40)
    print("📊 查询1：事件汇总（近 7 天）")
    print("=" * 40)
    df_summary = fetch_event_summary(days_back=7)
    print(df_summary.head(20).to_string(index=False))
    export_csv(df_summary, "ga4_event_summary.csv")

    # 2. 取消订阅事件明细（近 7 天）
    print("=" * 40)
    print("📋 查询2：取消订阅事件明细（近 7 天）")
    print("=" * 40)
    df_detail = fetch_event_detail(
        days_back=7,
        event_names=[
            "cancel_sub_reason_view",
            "cancel_sub_submit",
            "cancel_sub_success",
        ]
    )
    print(df_detail.head(10).to_string(index=False))
    export_csv(df_detail, "ga4_cancel_sub_detail.csv")

    print("🎉 完成！导出文件：ga4_event_summary.csv / ga4_cancel_sub_detail.csv")
```

---

## 五、常见问题

| 报错 | 原因 | 解决方法 |
|------|------|---------|
| `DefaultCredentialsError` | 未执行 gcloud 认证 | 重新执行第二步认证命令 |
| `403 Access Denied` | 账号没有 BigQuery 权限 | 联系管理员授权 |
| `PermissionError: CSV` | CSV 文件被 Excel 打开 | 关闭 Excel 后重新运行 |
| `ModuleNotFoundError` | 依赖包未安装 | 重新执行 pip install 命令 |
| `gcloud 无法识别` | 终端未刷新环境变量 | 重启终端或 VS Code |
