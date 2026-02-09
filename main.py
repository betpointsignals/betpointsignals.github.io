import psycopg2
import requests
import schedule
import time
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# PostgreSQL connection details
conn_details = {
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASSWORD"),
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT")
}

# OneSignal API details
ONE_SIGNAL_API_URL = "https://onesignal.com/api/v1/notifications"
ONE_SIGNAL_APP_ID = os.getenv("ONE_SIGNAL_APP_ID")
ONE_SIGNAL_API_KEY = os.getenv("ONE_SIGNAL_API_KEY")

# Memory to prevent duplicates per run
notified_ids = set()

def send_notification(message, player_id):
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Basic {ONE_SIGNAL_API_KEY}"
    }

    payload = {
        "app_id": ONE_SIGNAL_APP_ID,
        "include_player_ids": [player_id],  # 🎯 SINGLE DEVICE
        "headings": {"en": "NEW SIGNAL"},
        "contents": {"en": message}
    }

    try:
        response = requests.post(ONE_SIGNAL_API_URL, headers=headers, json=payload)
        if response.status_code == 200:
            logging.info(f"✅ Sent to device {player_id}")
        else:
            logging.error(f"❌ Failed for {player_id}: {response.status_code} {response.text}")
    except Exception as e:
        logging.error(f"❌ Error sending to {player_id}: {e}")

def check_new_rows():
    logging.info("🔍 Checking for new rows...")

    try:
        conn = psycopg2.connect(**conn_details)
        cursor = conn.cursor()

        # 1️⃣ Get signals
        cursor.execute("""
            SELECT 
                id,
                home_team,
                away_team,
                refer_team,
                signal,
                league
            FROM signal_main
            WHERE signal IS NOT NULL
            ORDER BY date_time DESC
        """)

        signals = cursor.fetchall()

        # 2️⃣ Get all device player_ids
        cursor.execute("""
            SELECT player_id
            FROM onesignal_users
            WHERE player_id IS NOT NULL
        """)

        devices = cursor.fetchall()

        cursor.close()
        conn.close()

        for signal_row in signals:
            (
                row_id,
                home_team,
                away_team,
                refer_team,
                signal,
                league
            ) = signal_row

            if row_id in notified_ids:
                continue

            opponent = away_team if refer_team == home_team else home_team

            message = (
                f"{refer_team} {signal}\n"
                f"vs {opponent}\n"
                f"{league}"
            )

            # 3️⃣ Send to EACH device
            for (player_id,) in devices:
                send_notification(message, player_id)

            notified_ids.add(row_id)
            logging.info(f"📣 Signal {row_id} sent to {len(devices)} devices")

    except Exception as e:
        logging.error(f"❌ Worker error: {e}")

schedule.every(10).seconds.do(check_new_rows)

logging.info("🚀 Worker started...")
while True:
    schedule.run_pending()
    time.sleep(1)
