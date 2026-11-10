"""
Messages privés quand une tâche Airflow casse, puis quand elle remarche.
Un seul message par panne (pas un par passage), envoyé sur Discord et/ou Telegram selon les variables :
  ALERT_DISCORD_WEBHOOK_URL            webhook d'un salon privé
  TELEGRAM_BOT_TOKEN + TELEGRAM_ALERT_CHAT_ID
  DISCORD_BOT_TOKEN + DISCORD_ALERT_USER_ID   message privé Discord
Sans variable configurée, rien n'est envoyé.
"""
import logging
import os

import dotenv
import requests

dotenv.load_dotenv()

HTTP_TIMEOUT = 15


def send_ops_message(message: str) -> int:
    """Send the message to every configured private channel. Returns how many accepted it."""
    sent = 0
    webhook = os.getenv("ALERT_DISCORD_WEBHOOK_URL")
    if webhook:
        try:
            requests.post(webhook, json={"content": message[:1900]}, timeout=HTTP_TIMEOUT).raise_for_status()
            sent += 1
        except Exception as e:
            logging.error(f"[ALERT] Discord alert failed: {e}")
    token, chat_id = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_ALERT_CHAT_ID")
    if token and chat_id:
        try:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": message[:4000], "disable_web_page_preview": True},
                timeout=HTTP_TIMEOUT,
            ).raise_for_status()
            sent += 1
        except Exception as e:
            logging.error(f"[ALERT] Telegram alert failed: {e}")
    bot_token, user_id = os.getenv("DISCORD_BOT_TOKEN"), os.getenv("DISCORD_ALERT_USER_ID")
    if bot_token and user_id:
        try:
            # Message privé : le bot ouvre (ou retrouve) la conversation, puis y écrit
            headers = {"Authorization": f"Bot {bot_token}"}
            channel = requests.post(
                "https://discord.com/api/v10/users/@me/channels",
                headers=headers, json={"recipient_id": user_id}, timeout=HTTP_TIMEOUT,
            )
            channel.raise_for_status()
            requests.post(
                f"https://discord.com/api/v10/channels/{channel.json()['id']}/messages",
                headers=headers, json={"content": message[:1900]}, timeout=HTTP_TIMEOUT,
            ).raise_for_status()
            sent += 1
        except Exception as e:
            logging.error(f"[ALERT] Discord DM alert failed: {e}")
    return sent


def _previous_state(context):
    previous = context["ti"].get_previous_ti()
    return previous.state if previous is not None else None


def notify_failure(context) -> None:
    """Task callback: alert on the first failed run of a streak only."""
    try:
        if _previous_state(context) == "failed":
            return
        ti = context["ti"]
        error = str(context.get("exception") or "see the task log")[:300]
        send_ops_message(
            f"❌ Starbase bot: '{ti.dag_id}' is failing\n"
            f"Run: {context.get('ts')}\n"
            f"Error: {error}\n"
            f"Log: {ti.log_url}\n"
            f"No further message until it works again."
        )
    except Exception as e:
        # Une alerte ratée ne doit jamais faire échouer la tâche elle-même
        logging.error(f"[ALERT] notify_failure crashed: {e}")


def notify_recovery(context) -> None:
    """Task callback: one message when a task works again after a failure."""
    try:
        if _previous_state(context) != "failed":
            return
        send_ops_message(f"✅ Starbase bot: '{context['ti'].dag_id}' is working again ({context.get('ts')})")
    except Exception as e:
        logging.error(f"[ALERT] notify_recovery crashed: {e}")


# À fusionner dans les default_args de chaque DAG
ALERT_ARGS = {"on_failure_callback": notify_failure, "on_success_callback": notify_recovery}
