"""
Messages privés quand une tâche Airflow casse, puis quand elle remarche.
Un seul message par panne (pas un par passage, et seulement si elle dure : FAILURES_BEFORE_ALERT), envoyé sur Discord et/ou Telegram selon les variables :
  ALERT_DISCORD_WEBHOOK_URL            webhook d'un salon privé
  TELEGRAM_BOT_TOKEN + TELEGRAM_ALERT_CHAT_ID
  DISCORD_BOT_TOKEN + DISCORD_ALERT_USER_ID   message privé Discord
Sans variable configurée, rien n'est envoyé.
AIRFLOW_PUBLIC_URL (optionnel) : adresse publique de l'UI, pour que le lien vers le log soit cliquable.
"""
import glob
import logging
import os
import re
from typing import Optional

import dotenv
import requests

dotenv.load_dotenv()

HTTP_TIMEOUT = 15
COLOR_FAILURE = 0xE74C3C
COLOR_RECOVERY = 0x2ECC71
ERROR_LINE_MAX = 450

# Nombre de runs ratés d'affilée avant d'alerter (1 par défaut). La page de la ville se vide
# parfois quelques minutes : pour roadclosure on attend 3 runs, soit environ 6 minutes.
FAILURES_BEFORE_ALERT = {"roadclosure": 3}

# Ligne de log Airflow relayant la sortie du script : "[date] {subprocess.py:93} INFO - <sortie>"
_SUBPROCESS_LINE_RE = re.compile(r"\{subprocess\.py:\d+\} \w+ - (.*)$")
_BASH_OPERATOR_NOISE = ("Tmp dir root location", "Running command:", "Output:", "Command exited with return code")
_FRAME_RE = re.compile(r'^\s+File "([^"]+)", line (\d+), in (\S+)')
_LEVEL_PREFIX_RE = re.compile(r"^(ERROR|CRITICAL):[^:]*:")


def send_ops_message(message: str, embed: Optional[dict] = None) -> int:
    """
    Send the message to every configured private channel. Returns how many accepted it.
    Discord gets `embed` (a rich card) when provided; Telegram always gets the plain text.
    """
    sent = 0
    discord_payload = {"embeds": [embed]} if embed else {"content": message[:1900]}
    webhook = os.getenv("ALERT_DISCORD_WEBHOOK_URL")
    if webhook:
        try:
            requests.post(webhook, json=discord_payload, timeout=HTTP_TIMEOUT).raise_for_status()
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
                headers=headers, json=discord_payload, timeout=HTTP_TIMEOUT,
            ).raise_for_status()
            sent += 1
        except Exception as e:
            logging.error(f"[ALERT] Discord DM alert failed: {e}")
    return sent


def _previous_states(ti, limit: int) -> list:
    """States of this task in the `limit` runs before this one, most recent first."""
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.session import create_session

    with create_session() as session:
        current = session.query(DagRun.execution_date).filter(
            DagRun.dag_id == ti.dag_id, DagRun.run_id == ti.run_id
        ).scalar()
        rows = (
            session.query(TaskInstance.state)
            .join(DagRun, (DagRun.dag_id == TaskInstance.dag_id) & (DagRun.run_id == TaskInstance.run_id))
            .filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.task_id == ti.task_id,
                DagRun.execution_date < current,
            )
            .order_by(DagRun.execution_date.desc())
            .limit(limit)
            .all()
        )
    return [str(state) if state is not None else None for (state,) in rows]


def _failed_runs_before(ti, limit: int) -> int:
    """How many runs in a row had failed just before this one (counted up to `limit`)."""
    count = 0
    for state in _previous_states(ti, limit):
        if state != "failed":
            break
        count += 1
    return count


def _threshold(ti) -> int:
    return FAILURES_BEFORE_ALERT.get(ti.dag_id, 1)


def _log_url(ti) -> str:
    """Task log link; the default base (localhost) is swapped for AIRFLOW_PUBLIC_URL when set."""
    url = ti.log_url or ""
    public = (os.getenv("AIRFLOW_PUBLIC_URL") or "").rstrip("/")
    if public and "/dags/" in url:
        url = public + url[url.index("/dags/"):]
    return url


def _script_output(ti) -> list:
    """Lines printed by the script itself, read back from the newest log file of this task."""
    from airflow.configuration import conf

    folder = os.path.join(
        conf.get("logging", "base_log_folder"), f"dag_id={ti.dag_id}", f"run_id={ti.run_id}", f"task_id={ti.task_id}"
    )
    attempts = glob.glob(os.path.join(folder, "attempt=*.log"))
    if not attempts:
        return []
    with open(max(attempts, key=os.path.getmtime), encoding="utf-8", errors="replace") as f:
        lines = [m.group(1) for m in map(_SUBPROCESS_LINE_RE.search, f) if m]
    return [line for line in lines if line.strip() and not line.startswith(_BASH_OPERATOR_NOISE)]


def explain_failure(lines: list) -> str:
    """
    Pick what explains the failure in a script's output: the exception that ended a traceback
    (with where it was raised in our code), else its last logged errors, else its last lines.
    """
    tracebacks = [i for i, line in enumerate(lines) if line.startswith("Traceback (most recent call last)")]
    if tracebacks:
        trace = lines[tracebacks[-1] + 1:]
        raised = [line for line in trace if not line.startswith(" ")]
        frames = [m for m in map(_FRAME_RE.search, trace) if m and "site-packages" not in m.group(1)]
        picked = raised[-1:] or trace[-1:]
        if frames:
            path, lineno, func = frames[-1].groups()
            picked.append(f"at {os.path.basename(path)}:{lineno} in {func}()")
    else:
        errors = [_LEVEL_PREFIX_RE.sub("", line) for line in lines if line.startswith(("ERROR:", "CRITICAL:"))]
        picked = errors[-2:] or lines[-3:]
    return "\n".join(line[:ERROR_LINE_MAX] for line in picked)


def _failure_reason(context) -> str:
    """The script's own error when the log gives one, else the (generic) Airflow exception."""
    try:
        reason = explain_failure(_script_output(context["ti"]))
        if reason:
            return reason
    except Exception as e:
        logging.warning(f"[ALERT] Could not read the task log: {e}")
    return str(context.get("exception") or "see the task log")[:ERROR_LINE_MAX]


def _run_field(context) -> dict:
    # <t:...> : Discord affiche la date dans le fuseau de celui qui lit
    when = context.get("logical_date")
    value = f"<t:{int(when.timestamp())}:f>" if when is not None else str(context.get("ts"))
    return {"name": "Run", "value": value, "inline": True}


def notify_failure(context) -> None:
    """Task callback: one alert per failure streak, sent once it reaches the DAG's threshold."""
    try:
        ti = context["ti"]
        threshold = _threshold(ti)
        # Seul le run qui atteint le seuil alerte : ni les coupures plus courtes, ni les runs suivants
        if _failed_runs_before(ti, threshold) != threshold - 1:
            return
        error = _failure_reason(context)
        log_url = _log_url(ti)
        embed = {
            "title": f"❌ {ti.dag_id} is failing",
            "description": f"```\n{error}\n```",
            "color": COLOR_FAILURE,
            "fields": [
                {"name": "Task", "value": f"`{ti.task_id}`", "inline": True},
                _run_field(context),
                {"name": "Log", "value": f"[Open in Airflow]({log_url})", "inline": True},
            ],
            "footer": {"text": "Starbase bot · no further message until it works again"},
        }
        send_ops_message(
            f"❌ Starbase bot: '{ti.dag_id}' is failing\n"
            f"Run: {context.get('ts')}\n"
            f"Error: {error}\n"
            f"Log: {log_url}\n"
            f"No further message until it works again.",
            embed=embed,
        )
    except Exception as e:
        # Une alerte ratée ne doit jamais faire échouer la tâche elle-même
        logging.error(f"[ALERT] notify_failure crashed: {e}")


def notify_recovery(context) -> None:
    """Task callback: one message when a task works again after a failure that was alerted."""
    try:
        ti = context["ti"]
        threshold = _threshold(ti)
        # Pas de "ça remarche" pour une panne restée sous le seuil d'alerte
        if _failed_runs_before(ti, threshold) < threshold:
            return
        embed = {
            "title": f"✅ {ti.dag_id} is working again",
            "color": COLOR_RECOVERY,
            "fields": [{"name": "Task", "value": f"`{ti.task_id}`", "inline": True}, _run_field(context)],
            "footer": {"text": "Starbase bot"},
        }
        send_ops_message(f"✅ Starbase bot: '{ti.dag_id}' is working again ({context.get('ts')})", embed=embed)
    except Exception as e:
        logging.error(f"[ALERT] notify_recovery crashed: {e}")


# À fusionner dans les default_args de chaque DAG
ALERT_ARGS = {"on_failure_callback": notify_failure, "on_success_callback": notify_recovery}
