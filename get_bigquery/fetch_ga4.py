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