"""TFR, maritime notices, flight tracker and digest (no network, no Mongo, no Twitter)."""
import json
import os
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from Digest import build_digest_post  # noqa: E402
from FlightTracker import (  # noqa: E402
    beach_signal,
    build_tracker_post,
    describe_change,
    is_major_change,
    make_signature,
    marine_signal,
    pick_tracked_launch,
    tfr_signal,
)
from NavWarnings import (  # noqa: E402
    build_warning_post,
    collect_starbase_warnings,
    parse_coordinates,
    parse_warning_windows,
)
from TFR import build_tfr_post, is_starbase_space_tfr, parse_tfr_detail  # noqa: E402
from utils.launches import simplify_launch  # noqa: E402
from utils.notify import tweet_len  # noqa: E402
from utils.timeparse import UTC, fmt_utc  # noqa: E402
from utils.tracking import plan_tracked  # noqa: E402

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
NET = datetime(2026, 11, 13, 22, 45, tzinfo=UTC)


def _types(actions):
    return [a["type"] for a in actions]


def _launch(**extra):
    launch = simplify_launch({
        "id": "abc",
        "name": "Starship | Starlink Group 31-2 (Starship Flight 15)",
        "net": "2026-11-13T22:45:00Z",
        "net_precision": {"name": "Minute"},
        "status": {"abbrev": "Go", "name": "Go for Launch"},
        "pad": {"name": "Orbital Launch Mount B", "location": {"name": "SpaceX Starbase, TX, USA"}},
    })
    launch.update(extra)
    return launch


class TrackingTests(unittest.TestCase):
    def _doc(self, key, end, **extra):
        return {key: dict({"uniq_key": key, "active": True, "end": end, "missing_count": 0}, **extra)}

    def test_new_and_seen(self):
        self.assertEqual(_types(plan_tracked({"a": {}}, {}, NOW)), ["new"])
        self.assertEqual(_types(plan_tracked({"a": {}}, self._doc("a", NET), NOW)), ["seen"])

    def test_reissued_notice_is_an_update(self):
        old = self._doc("a", NET, begin=NET - timedelta(hours=6))
        actions = plan_tracked({"b": {"begin": NET + timedelta(days=1)}}, old, NOW)
        self.assertEqual(_types(actions), ["updated"])
        self.assertEqual(actions[0]["previous"]["uniq_key"], "a")

    def test_unrelated_notices_are_not_an_update(self):
        old = self._doc("a", NET, begin=NET - timedelta(hours=6))
        # Dates éloignées, dates inconnues, ou ancien avis déjà terminé : deux événements distincts
        far = plan_tracked({"b": {"begin": NET + timedelta(days=10)}}, old, NOW)
        self.assertEqual(_types(far), ["new", "missing"])
        self.assertEqual(_types(plan_tracked({"b": {}}, old, NOW)), ["new", "missing"])
        ended = self._doc("a", NOW - timedelta(hours=1), begin=NOW - timedelta(hours=8))
        self.assertEqual(_types(plan_tracked({"b": {"begin": NOW + timedelta(hours=20)}}, ended, NOW)), ["new", "missing"])

    def test_removed_only_after_three_runs_and_before_the_end(self):
        self.assertEqual(_types(plan_tracked({}, self._doc("a", NET), NOW)), ["missing"])
        self.assertEqual(_types(plan_tracked({}, self._doc("a", NET, missing_count=2), NOW)), ["removed"])
        ended = NOW - timedelta(hours=1)
        self.assertEqual(_types(plan_tracked({}, self._doc("a", ended, missing_count=2), NOW)), ["expired"])
        self.assertEqual(_types(plan_tracked({}, self._doc("a", None, missing_count=2), NOW)), ["expired"])

    def test_inactive_docs_are_ignored(self):
        self.assertEqual(plan_tracked({}, self._doc("a", NET, active=False), NOW), [])


class TfrTests(unittest.TestCase):
    DETAIL = (
        "FDC 6/7001\nBeginning Date and Time : November 13, 2026 at 2145 UTC\n"
        "Ending Date and Time : November 14, 2026 at 0130 UTC\n"
        "Altitude: From the surface up to unlimited.\nReason for NOTAM : Space Operations Area"
    )

    def test_filter(self):
        space = {"type": "SPACE OPERATIONS", "description": "Brownsville, TX, Friday, November 13, 2026 UTC"}
        self.assertTrue(is_starbase_space_tfr(space))
        self.assertFalse(is_starbase_space_tfr(dict(space, type="SECURITY")))
        self.assertFalse(is_starbase_space_tfr(dict(space, description="Cape Canaveral, FL, Friday")))

    def test_detail(self):
        detail = parse_tfr_detail(self.DETAIL)
        self.assertEqual(detail["begin"], datetime(2026, 11, 13, 21, 45, tzinfo=UTC))
        self.assertEqual(detail["end"], datetime(2026, 11, 14, 1, 30, tzinfo=UTC))
        self.assertEqual(detail["altitude"], "From the surface up to unlimited")
        self.assertEqual(parse_tfr_detail("nothing useful"), {"begin": None, "end": None, "altitude": None})

    def test_posts(self):
        item = dict(parse_tfr_detail(self.DETAIL), notam_id="6/7001", location="Brownsville, TX", description="raw")
        for event in ("new", "updated", "removed"):
            text, card = build_tfr_post(event, item, previous={"notam_id": "6/6990"}, now=NOW)
            self.assertLessEqual(tweet_len(text), 280)
            self.assertIn("6/7001", text)
            self.assertTrue(card and card.startswith(b"\x89PNG"))
        text, _ = build_tfr_post("new", item, now=NOW)
        self.assertIn("🌐 Nov 13, 21:45 → Nov 14, 01:30 UTC", text)
        # Sans détail lisible, la description brute de la FAA est utilisée
        text, _ = build_tfr_post("new", dict(item, begin=None, end=None), now=NOW)
        self.assertIn("raw", text)


class NavWarningTests(unittest.TestCase):
    """Runs on a hand-written sample of the NGA format (see the fixture's _comment)."""

    def setUp(self):
        with open(os.path.join(FIXTURES, "nga_broadcast_warn_synthetic.json"), encoding="utf-8") as f:
            self.warnings = json.load(f)["broadcast-warn"]

    def test_coordinates(self):
        (lat, lon), = parse_coordinates("26-03.00N 097-08.00W")
        self.assertAlmostEqual(lat, 26.05)
        self.assertAlmostEqual(lon, -97.1333, places=3)

    def test_windows(self):
        windows = parse_warning_windows(self.warnings[0]["text"], 2026)
        self.assertEqual(len(windows), 3)
        self.assertEqual(windows[0], (datetime(2026, 11, 13, 22, 45, tzinfo=UTC), datetime(2026, 11, 14, 1, 28, tzinfo=UTC)))

    def test_window_over_a_month_change(self):
        (start, end), = parse_warning_windows("ROCKET LAUNCHING 302245Z TO 010128Z DEC", 2026)
        self.assertEqual((start.month, start.day, end.month, end.day), (11, 30, 12, 1))

    def test_only_the_starbase_launch_notice_is_kept(self):
        items = collect_starbase_warnings(self.warnings, NOW)
        self.assertEqual(list(items), ["4-1187/2026"])
        item = items["4-1187/2026"]
        self.assertEqual(item["reference"], "NAVAREA IV 1187/26")
        self.assertEqual(item["end"], datetime(2026, 11, 16, 1, 28, tzinfo=UTC))

    def test_posts(self):
        item = collect_starbase_warnings(self.warnings, NOW)["4-1187/2026"]
        for event in ("new", "updated", "removed"):
            text, card = build_warning_post(event, item, previous={"reference": "NAVAREA IV 1180/26"}, now=NOW)
            self.assertLessEqual(tweet_len(text), 280)
            self.assertIn("NAVAREA IV 1187/26", text)
            self.assertTrue(card and card.startswith(b"\x89PNG"))
        text, _ = build_warning_post("new", item, now=NOW)
        self.assertIn("🌐 Nov 13, 22:45 → Nov 14, 01:28 UTC", text)
        self.assertIn("+2 alternate windows", text)


class FlightTrackerTests(unittest.TestCase):
    def test_launch_is_simplified(self):
        launch = _launch()
        self.assertEqual(launch["label"], "Starship Flight 15")
        self.assertEqual(launch["net"], NET)
        self.assertTrue(launch["firm"] and launch["has_time"] and launch["at_starbase"])
        self.assertIsNone(simplify_launch({"name": "Starship", "net": None}))

    def test_pick(self):
        self.assertIsNotNone(pick_tracked_launch([_launch()], NOW))
        self.assertIsNone(pick_tracked_launch([_launch(firm=False)], NOW))
        self.assertIsNone(pick_tracked_launch([_launch(at_starbase=False)], NOW))
        self.assertIsNone(pick_tracked_launch([_launch()], NOW - timedelta(days=60)))

    def test_signals(self):
        beach = [{"category": "beach", "date": "Nov. 13 8:00 AM to Nov. 13 8:00 PM", "first_seen": NOW}]
        self.assertEqual(beach_signal(beach, NET, NOW), "Nov 13")
        self.assertIsNone(beach_signal([dict(beach[0], date="Nov. 20 8:00 AM to Nov. 20 8:00 PM")], NET, NOW))
        self.assertIsNone(beach_signal([dict(beach[0], category="road")], NET, NOW))

        tfr = [{"notam_id": "6/7001", "begin": NET - timedelta(hours=1), "end": NET + timedelta(hours=3)}]
        self.assertEqual(tfr_signal(tfr, NET), "FDC 6/7001")
        self.assertIsNone(tfr_signal(tfr, NET + timedelta(days=5)))

        marine = [{"reference": "NAVAREA IV 1187/26", "windows": [[NET - timedelta(hours=1), NET + timedelta(hours=3)]]}]
        self.assertEqual(marine_signal(marine, NET), "NAVAREA IV 1187/26")
        self.assertIsNone(marine_signal(marine, NET + timedelta(days=5)))

    def test_signature_changes_with_date_and_signals(self):
        none = {"beach": None, "tfr": None, "marine": None}
        base = make_signature(_launch(), none)
        self.assertEqual(base, make_signature(_launch(), dict(none)))
        self.assertNotEqual(base, make_signature(_launch(), dict(none, tfr="FDC 6/7001")))
        self.assertNotEqual(base, make_signature(_launch(net=NET + timedelta(days=1)), none))

    def test_only_status_changes_are_minor(self):
        none = {"beach": None, "tfr": None, "marine": None}
        state = {"net": NET, "signals": none, "status": "Go"}
        self.assertTrue(is_major_change(_launch(), none, None))
        self.assertTrue(is_major_change(_launch(), dict(none, tfr="FDC 6/7001"), state))
        self.assertTrue(is_major_change(_launch(), none, dict(state, signals=dict(none, beach="Nov 13"))))
        self.assertTrue(is_major_change(_launch(net=NET + timedelta(days=1)), none, state))
        self.assertFalse(is_major_change(_launch(status="TBC", status_name="To Be Confirmed"), none, state))

    def test_describe_change(self):
        none = {"beach": None, "tfr": None, "marine": None}
        state = {"net": NET, "signals": none, "status": "Go"}
        self.assertEqual(describe_change(_launch(), none, None), "")
        self.assertEqual(describe_change(_launch(), none, state), "")
        self.assertIn("Airspace TFR posted", describe_change(_launch(), dict(none, tfr="FDC 6/7001"), state))
        self.assertIn("withdrawn", describe_change(_launch(), none, dict(state, signals=dict(none, beach="Nov 13"))))
        self.assertIn("Date moved", describe_change(_launch(net=NET + timedelta(days=1)), none, state))
        self.assertIn("Status now", describe_change(_launch(status="TBC", status_name="To Be Confirmed"), none, state))

    def test_post(self):
        signals = {"beach": "Nov 13", "tfr": "FDC 6/7001", "marine": None}
        text, card = build_tracker_post(_launch(), signals, now=NOW)
        self.assertLessEqual(tweet_len(text), 280)
        self.assertIn("Starship Flight 15", text)
        self.assertIn("✅ Beach closure: Nov 13", text)
        self.assertIn("⬜ Marine hazard area: not posted yet", text)
        self.assertIn("T-3d 10h", text)
        self.assertTrue(card and card.startswith(b"\x89PNG"))


class DigestTests(unittest.TestCase):
    def test_quiet_day(self):
        text, card = build_digest_post([], [], [], None, now=NOW)
        self.assertIn("Starbase today · Tue, Nov 10", text)
        self.assertIn("Beach: no closure listed", text)
        self.assertIn("no Starship launch dated yet", text)
        self.assertLessEqual(tweet_len(text), 280)
        self.assertTrue(card and card.startswith(b"\x89PNG"))

    def test_busy_day(self):
        closures = [
            {"category": "beach", "date": "Nov. 13 8:00 AM to Nov. 13 8:00 PM", "first_seen": NOW},
            {"category": "road", "date": "November 11 10:00 PM to November 12 4:30 AM", "first_seen": NOW},
        ]
        tfrs = [{"notam_id": "6/7001", "begin": NET - timedelta(hours=1), "end": NET + timedelta(hours=3)}]
        marine = [{"reference": "NAVAREA IV 1187/26", "windows": [[NET - timedelta(hours=1), NET + timedelta(hours=3)]]}]
        text, _ = build_digest_post(closures, tfrs, marine, _launch(), now=NOW)
        self.assertIn("Beach: closure Nov 13, 8:00 AM → 8:00 PM CT", text)
        self.assertIn("Road: 1 delay listed", text)
        # Trop long pour un post : version compacte, sans rien tronquer
        self.assertIn("Flight 15 NET Nov 13, 22:45 UTC", text)
        self.assertNotIn("…", text)
        self.assertLessEqual(tweet_len(text), 280)


if __name__ == "__main__":
    unittest.main()


class DigestLineTests(unittest.TestCase):
    def test_every_beach_closure_is_counted(self):
        from Digest import beach_line
        closures = [
            {"category": "beach", "date": "Nov. 14 11:00 AM to Nov. 14 11:30 PM", "first_seen": NOW},
            {"category": "beach", "date": "Nov. 13 11:00 AM to Nov. 13 11:30 PM", "first_seen": NOW},
        ]
        self.assertEqual(beach_line(closures, NOW), "2 closures listed, next Nov 13, 11:00 AM → 11:30 PM CT")

    def test_nearest_tfr_first(self):
        from Digest import tfr_line
        tfrs = [
            {"notam_id": "6/7132", "begin": NET + timedelta(days=2), "end": NET + timedelta(days=2, hours=3)},
            {"notam_id": "6/7129", "begin": NET, "end": NET + timedelta(hours=2)},
            {"notam_id": "6/9999"},
        ]
        self.assertTrue(tfr_line(tfrs).startswith(f"3 active, next {fmt_utc((NET, NET + timedelta(hours=2)))}"))


class WeeklyTests(unittest.TestCase):
    START = NOW - timedelta(days=7)

    def _stats(self):
        from Weekly import week_stats
        inside, before = NOW - timedelta(days=2), NOW - timedelta(days=9)
        closures = [
            {"category": "beach", "first_seen": inside},
            {"category": "beach", "first_seen": inside, "superseded_by": "x"},   # reportée : comptée via son successeur
            {"category": "beach", "first_seen": before, "cancelled_at": inside},
            {"category": "road", "first_seen": inside},
            {"first_seen": before},          # hors période
            {"category": "road"},            # ancien doc sans date
        ]
        tfrs = [{"first_seen": inside, "removed_at": inside}]
        return week_stats(closures, tfrs, [], self.START, NOW)

    def test_stats(self):
        stats = self._stats()
        self.assertEqual((stats["beach_new"], stats["beach_cancelled"], stats["road_new"]), (1, 1, 1))
        self.assertEqual((stats["tfr_new"], stats["tfr_removed"], stats["marine_new"]), (1, 1, 0))

    def test_post(self):
        from Weekly import build_weekly_post
        text, card = build_weekly_post(self._stats(), _launch(), self.START, NOW, now=NOW)
        self.assertIn("Starbase week · Nov 3 → Nov 10", text)
        self.assertIn("Beach closures: 1 posted, 1 cancelled", text)
        self.assertNotIn("Marine", text)
        self.assertLessEqual(tweet_len(text), 280)
        self.assertTrue(card and card.startswith(b"\x89PNG"))

    def test_quiet_week_posts_nothing(self):
        from Weekly import build_weekly_post, week_stats
        quiet = week_stats([{"category": "road", "first_seen": NOW - timedelta(days=1)}] * 2, [], [], self.START, NOW)
        self.assertIsNone(build_weekly_post(quiet, _launch(), self.START, NOW, now=NOW))
