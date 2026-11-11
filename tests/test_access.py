"""Road / beach access: date parsing, extraction, change detection, post format.

Run from the repo root:  python -m unittest discover -s tests
"""
import os
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from Scraper_RC import (  # noqa: E402
    PageStructureError,
    build_post_card,
    build_post_text,
    check_page_structure,
    extract_access_updates,
    plan_changes,
    records_with_keys,
)
from utils.notify import fit_lines, tweet_len  # noqa: E402
from utils.timeparse import (  # noqa: E402
    STARBASE_TZ,
    UTC,
    fmt_duration,
    fmt_local,
    fmt_utc,
    parse_window,
    parse_windows,
    relative_label,
)

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
NOW = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)


def _fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


def _beach(date="Oct. 25 6:00 AM to Oct. 25 6:00 PM", backup=None, **extra):
    record = {
        "category": "beach",
        "status": "Beach closure",
        "description": "Boca Chica Beach",
        "date": date,
        "backup_date": backup,
        "uniq_key": f"beach::{date.lower()}::boca chica beach" + (f"::{backup.lower()}" if backup else ""),
    }
    record.update(extra)
    return record


def _stored(record, **extra):
    doc = dict(record, active=True, first_seen=NOW - timedelta(hours=6), last_seen=NOW, missing_count=0)
    doc.update(extra)
    return {doc["uniq_key"]: doc}


def _types(actions):
    return [a["type"] for a in actions]


class TimeParseTests(unittest.TestCase):
    def test_long_month_overnight(self):
        start, end = parse_window("October 25 10:00 PM to October 26 4:30 AM", now=NOW)
        self.assertEqual(start, datetime(2026, 10, 25, 22, 0, tzinfo=STARBASE_TZ))
        self.assertEqual(end, datetime(2026, 10, 26, 4, 30, tzinfo=STARBASE_TZ))

    def test_abbreviated_month(self):
        window = parse_window("Oct. 25 6:00 AM to Oct. 25 6:00 PM", now=NOW)
        self.assertEqual(fmt_local(window), "Oct 25, 6:00 AM → 6:00 PM CT")
        self.assertEqual(fmt_utc(window), "Oct 25, 11:00 → 23:00 UTC")

    def test_year_is_the_closest_one(self):
        january = datetime(2027, 1, 2, tzinfo=UTC)
        self.assertEqual(parse_window("Dec. 30 8:00 AM to Dec. 30 8:00 PM", now=january)[0].year, 2026)
        december = datetime(2026, 12, 30, tzinfo=UTC)
        self.assertEqual(parse_window("Jan. 2 8:00 AM to Jan. 2 8:00 PM", now=december)[0].year, 2027)

    def test_unreadable_text(self):
        self.assertIsNone(parse_window("To be announced", now=NOW))
        self.assertIsNone(parse_window(None, now=NOW))
        self.assertEqual(parse_windows(["nope", None], now=NOW), [])

    def test_backup_list(self):
        windows = parse_windows(
            ["Oct. 25 6:00 AM to Oct. 25 6:00 PM", "Oct. 26 6:00 AM to Oct. 26 6:00 PM | Oct. 27 6:00 AM to Oct. 27 6:00 PM"],
            now=NOW,
        )
        self.assertEqual(len(windows), 3)

    def test_durations(self):
        self.assertEqual(fmt_duration(timedelta(days=2, hours=3, minutes=10)), "2d 3h")
        self.assertEqual(fmt_duration(timedelta(hours=14, minutes=20)), "14h")
        self.assertEqual(fmt_duration(timedelta(minutes=45)), "45 min")
        self.assertEqual(fmt_duration(timedelta(minutes=-5)), "0 min")

    def test_relative_label(self):
        window = parse_window("Oct. 25 6:00 AM to Oct. 25 6:00 PM", now=NOW)
        self.assertTrue(relative_label(window, NOW).startswith("starts in"))
        self.assertTrue(relative_label(window, window[0] + timedelta(hours=1)).startswith("in progress"))
        self.assertEqual(relative_label(window, window[1]), "ended")


class ExtractionTests(unittest.TestCase):
    def test_page_with_closures(self):
        html = _fixture("access_closures_2026-10-25.html")
        check_page_structure(html)
        records = records_with_keys(extract_access_updates(html, source_url="https://example.test"))
        categories = {r["category"] for r in records}
        self.assertEqual(categories, {"beach", "road"})
        self.assertEqual(len({r["uniq_key"] for r in records}), len(records))
        # Toutes les dates réelles de la page doivent être comprises par le parseur
        for record in records:
            self.assertIsNotNone(parse_window(record["date"], now=NOW), record["date"])
            text = build_post_text("new", record, now=NOW)
            self.assertLessEqual(tweet_len(text), 280)
            self.assertIn("NEW", text)

    def test_empty_page_is_valid(self):
        html = _fixture("access_empty_2026-11-10.html")
        check_page_structure(html)
        self.assertEqual(records_with_keys(extract_access_updates(html)), [])

    def test_changed_layout_is_detected(self):
        with self.assertRaises(PageStructureError):
            check_page_structure("<html><body><h1>Maintenance</h1></body></html>")
        with self.assertRaises(PageStructureError):
            check_page_structure('<html><body><div id="road-closure"></div><div id="beach-closure"></div></body></html>')

    def test_webflow_empty_state_is_a_valid_empty_page(self):
        # État "liste vide" du site : "Boca Chica Beach is open." / "No Road Delay." sans aucun .cms-big-text
        html = _fixture("access_dyn_empty_2026-11-10.html")
        check_page_structure(html)
        self.assertEqual(records_with_keys(extract_access_updates(html)), [])


    def test_several_closures_in_one_block(self):
        # La ville liste deux fermetures dans le même bloc : "Primary: Nov. 13 ..." puis "Primary: Nov. 14 ..."
        records = records_with_keys(extract_access_updates(_fixture("access_two_beach_closures_2026-11-11.html")))
        self.assertEqual(
            [r["date"] for r in records],
            ["Nov. 13 11:00 AM to Nov. 13 11:30 PM", "Nov. 14 11:00 AM to Nov. 14 11:30 PM"],
        )
        self.assertTrue(all(r["category"] == "beach" and not r["backup_date"] for r in records))

    def test_backup_stays_with_its_own_primary(self):
        html = (
            '<div id="beach-closure"><div class="w-dyn-item"><div class="cms-big-text">Boca Chica Beach closures.</div>'
            '<div class="cms-small-text-api">Primary: Nov. 13 11:00 AM to Nov. 13 11:30 PM<br>'
            'Backup: Nov. 14 11:00 AM to Nov. 14 11:30 PM<br>Primary: Nov. 20 8:00 AM to Nov. 20 8:00 PM</div>'
            '</div></div><div id="road-closure"></div>'
        )
        records = records_with_keys(extract_access_updates(html))
        self.assertEqual(
            [(r["date"], r["backup_date"]) for r in records],
            [
                ("Nov. 13 11:00 AM to Nov. 13 11:30 PM", "Nov. 14 11:00 AM to Nov. 14 11:30 PM"),
                ("Nov. 20 8:00 AM to Nov. 20 8:00 PM", None),
            ],
        )


class PlanChangesTests(unittest.TestCase):
    def test_new(self):
        self.assertEqual(_types(plan_changes([_beach()], {}, NOW)), ["new"])

    def test_seen(self):
        record = _beach()
        self.assertEqual(_types(plan_changes([record], _stored(record), NOW)), ["seen"])

    def test_legacy_doc_without_active_field_is_not_reposted(self):
        record = _beach()
        legacy = {record["uniq_key"]: {k: record[k] for k in ("category", "description", "date", "uniq_key")}}
        self.assertEqual(_types(plan_changes([record], legacy, NOW)), ["seen"])
        # ... et un ancien doc absent de la page n'est jamais annoncé comme annulé
        self.assertEqual(plan_changes([], legacy, NOW), [])

    def test_updated_when_date_moves(self):
        old = _beach()
        new = _beach(date="Oct. 26 6:00 AM to Oct. 26 6:00 PM")
        actions = plan_changes([new], _stored(old), NOW)
        self.assertEqual(_types(actions), ["updated"])
        self.assertEqual(actions[0]["previous"]["uniq_key"], old["uniq_key"])

    def test_other_description_is_not_an_update(self):
        old = _beach()
        road = {"category": "road", "status": "Road Delay", "description": "Masseys to Production",
                "date": "October 25 10:00 PM to October 26 4:30 AM", "backup_date": None, "uniq_key": "road-key"}
        self.assertEqual(_types(plan_changes([road], _stored(old), NOW)), ["new", "missing"])

    def test_second_closure_listed_next_to_the_first_is_new(self):
        first, second = _beach(), _beach(date="Oct. 26 6:00 AM to Oct. 26 6:00 PM")
        self.assertEqual(_types(plan_changes([first, second], _stored(first), NOW)), ["seen", "new"])

    def test_ended_closure_is_not_rescheduled(self):
        old = _beach(date="Oct. 23 6:00 AM to Oct. 23 6:00 PM")
        new = _beach(date="Oct. 30 6:00 AM to Oct. 30 6:00 PM")
        self.assertEqual(_types(plan_changes([new], _stored(old), NOW)), ["new", "missing"])

    def test_same_dates_written_differently_is_silent(self):
        old = _beach(date="Oct. 25 6:00 AM to Oct. 25 6:00 PM")
        for new in (
            _beach(date="Oct 25 6:00 AM to Oct 25 6:00 PM"),
            _beach(description="Boca Chica Beach closures", uniq_key="beach::oct. 25 6:00 am to oct. 25 6:00 pm::boca chica beach closures"),
        ):
            actions = plan_changes([new], _stored(old), NOW)
            self.assertEqual(_types(actions), ["rekeyed"])
            self.assertEqual(actions[0]["previous"]["uniq_key"], old["uniq_key"])

    def test_beach_wording_change_with_new_date_is_an_update(self):
        old = _beach()
        new = _beach(date="Oct. 26 6:00 AM to Oct. 26 6:00 PM", description="Boca Chica Beach closures")
        self.assertEqual(_types(plan_changes([new], _stored(old), NOW)), ["updated"])

    def test_scrolling_banner_is_not_read(self):
        html = (
            '<div id="beach-closure"></div><div id="road-closure"></div>'
            '<div class="notification-bar"><div id="rich-bar-message">test | delay</div></div>'
        )
        self.assertEqual(records_with_keys(extract_access_updates(html)), [])

    def test_missing_needs_three_runs(self):
        record = _beach()
        for count, expected in ((0, "missing"), (1, "missing"), (2, "cancelled")):
            actions = plan_changes([], _stored(record, missing_count=count), NOW)
            self.assertEqual(_types(actions), [expected])
        self.assertFalse(actions[0]["in_progress"])

    def test_removed_during_the_window_is_lifted(self):
        record = _beach()
        during = datetime(2026, 10, 25, 15, 0, tzinfo=UTC)
        actions = plan_changes([], _stored(record, missing_count=2), during)
        self.assertEqual(_types(actions), ["cancelled"])
        self.assertTrue(actions[0]["in_progress"])

    def test_gone_after_the_window_is_silent(self):
        record = _beach()
        after = datetime(2026, 10, 26, 12, 0, tzinfo=UTC)
        self.assertEqual(_types(plan_changes([], _stored(record, missing_count=2), after)), ["expired"])

    def test_unreadable_date_is_never_cancelled(self):
        record = _beach(date="To be announced")
        self.assertEqual(_types(plan_changes([], _stored(record, missing_count=2), NOW)), ["expired"])

    def test_relisted_after_cancellation_is_new(self):
        record = _beach()
        stored = _stored(record, active=False, cancelled_at=NOW - timedelta(hours=2))
        self.assertEqual(_types(plan_changes([record], stored, NOW)), ["new"])


class PostFormatTests(unittest.TestCase):
    def test_new_beach_post(self):
        text = build_post_text("new", _beach(backup="Oct. 26 6:00 AM to Oct. 26 6:00 PM"), now=NOW)
        lines = text.split("\n")
        self.assertIn("NEW · Boca Chica Beach closure", lines[0])
        self.assertIn("🕒 Primary: Oct 25, 6:00 AM → 6:00 PM CT", lines)
        self.assertIn("🕒 Backup: Oct 26, 6:00 AM → 6:00 PM CT", lines)
        self.assertIn("🌐 Oct 25, 11:00 → 23:00 UTC", lines)
        self.assertIn("Source: City of Starbase", lines)
        self.assertLessEqual(tweet_len(text), 280)

    def test_updated_post_shows_the_old_window(self):
        old = dict(_beach(), first_seen=NOW)
        text = build_post_text("updated", _beach(date="Oct. 26 6:00 AM to Oct. 26 6:00 PM"), previous=old, now=NOW)
        self.assertIn("UPDATED", text)
        self.assertIn("Was: Oct 25, 6:00 AM → 6:00 PM CT", text)

    def test_cancelled_and_lifted_posts(self):
        record = dict(_beach(), first_seen=NOW)
        cancelled = build_post_text("cancelled", record, now=NOW)
        self.assertIn("CANCELLED", cancelled)
        self.assertIn("before the window started", cancelled)
        lifted = build_post_text("lifted", record, now=datetime(2026, 10, 25, 15, 0, tzinfo=UTC))
        self.assertIn("LIFTED EARLY", lifted)
        self.assertIn("before the scheduled end", lifted)

    def test_unreadable_date_falls_back_to_raw_text(self):
        text = build_post_text("new", _beach(date="Sometime next week"), now=NOW)
        self.assertIn("Sometime next week (local time)", text)

    def test_long_road_description_still_fits(self):
        road = {"category": "road", "status": "Road Delay", "description": "Production to Masseys " * 12,
                "date": "October 25 10:00 PM to October 26 4:30 AM", "backup_date": None}
        self.assertLessEqual(tweet_len(build_post_text("new", road, now=NOW)), 280)

    def test_fit_lines_drops_least_important_first(self):
        text = fit_lines([("keep", 0), ("x" * 200, 5), ("y" * 100, 1)], limit=120)
        self.assertEqual(text, "keep\n" + "y" * 100)
        self.assertEqual(fit_lines([("a", 0), ("", 1), ("b", 0)]), "a\nb")

    def test_emoji_count_double(self):
        self.assertEqual(tweet_len("ab"), 2)
        self.assertEqual(tweet_len("🚀"), 2)

    def test_card_is_a_png(self):
        card = build_post_card("new", _beach(), now=NOW)
        self.assertTrue(card and card.startswith(b"\x89PNG"))


if __name__ == "__main__":
    unittest.main()
