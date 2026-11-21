"""The marginalpdbc contract on the recorded OMIE files and on variants OMIE never published."""

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise

import pytest

from ibergrid.sources.omie.marginalpdbc import (
    CUTOVER,
    V1,
    V2,
    InvalidFile,
    contract_for,
    expected_periods,
    outside_limits,
    parse,
    to_rows,
)

from .conftest import OMIE_FIXTURES, fixture, omie_file

# The recorded files, byte for byte as OMIE served them on 2026-11-21 (Latin-1, CRLF): a checkout
# or an editor that rewrote one would change what the other tests read.
FIXTURE_SHA256 = {
    "marginalpdbc_20230528.2": "b35ecf644c0dece286402f31d35dd9b9e0eeb9326a5fd62ce7e1afb8c42eb1f0",
    "marginalpdbc_20240331.1": "e6b0ed7942c991906584253e2f189958358be01298ccf0dac868afa43ddfd8f5",
    "marginalpdbc_20241027.1": "471e462c5b8f6fa94bc7d5d2a422dc5f252d931f4df13eb9cdd8c389e558301a",
    "marginalpdbc_20250428.1": "adbcd82a65abb28fc8776f98dbf31daa2496d50d8ec2186ced6d8a2737b427be",
    "marginalpdbc_20250930.1": "ad1b25b9c6059b1435e9700a7e751dc9339cac2ecfa7edfe20d2bd76a443260e",
    "marginalpdbc_20251001.1": "6f0b0a5c4f86a4c34eb68c528bc5f9b58987183d80d90ce5fa18458921c3bd0b",
    "marginalpdbc_20251026.1": "e0fce08b8883792a0755ff7c9887bfad4926c61cc98908ed802da12bb1ef5263",
    "marginalpdbc_20251030.3": "f6c344a5e49f2ba94c501715b9ac9f0247c057264e4a5b5d3a9bec62f9c2deb0",
    "marginalpdbc_20260329.1": "9159169cd6b18c158ee7c3a0cc1c9c588dc42282617ce697dc9f3c670c1b8f95",
    "marginalpdbc_20261118.1": "426a7115c81deb641e4d624ef113ef00c44294b10ca779c007dc7a5746eb6be4",
}


def day(iso: str) -> date:
    return date.fromisoformat(iso)


def lines(iso: str) -> list[str]:
    """A recorded file's lines, without their CRLF."""
    return omie_file(iso).decode("latin-1").split("\r\n")


def build(text_lines: list[str]) -> bytes:
    return "\r\n".join(text_lines).encode("latin-1")


def utc(year: int, month: int, dom: int, hour: int = 0, minute: int = 0) -> datetime:
    return datetime(year, month, dom, hour, minute, tzinfo=UTC)


def test_the_recorded_files_are_byte_exact() -> None:
    found = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in OMIE_FIXTURES.iterdir()
    }

    assert found == FIXTURE_SHA256


@pytest.mark.parametrize(
    ("iso", "version", "periods", "first_start", "last_end"),
    [
        ("2023-05-28", 1, 24, utc(2023, 5, 27, 22), utc(2023, 5, 28, 22)),
        ("2024-03-31", 1, 23, utc(2024, 3, 30, 23), utc(2024, 3, 31, 22)),
        ("2024-10-27", 1, 25, utc(2024, 10, 26, 22), utc(2024, 10, 27, 23)),
        ("2025-04-28", 1, 24, utc(2025, 4, 27, 22), utc(2025, 4, 28, 22)),
        ("2025-09-30", 1, 24, utc(2025, 9, 29, 22), utc(2025, 9, 30, 22)),
        ("2025-10-01", 2, 96, utc(2025, 9, 30, 22), utc(2025, 10, 1, 22)),
        ("2025-10-26", 2, 100, utc(2025, 10, 25, 22), utc(2025, 10, 26, 23)),
        ("2025-10-30", 2, 96, utc(2025, 10, 29, 23), utc(2025, 10, 30, 23)),
        ("2026-03-29", 2, 92, utc(2026, 3, 28, 23), utc(2026, 3, 29, 22)),
        ("2026-11-18", 2, 96, utc(2026, 11, 17, 23), utc(2026, 11, 18, 23)),
    ],
)
def test_every_recorded_file_parses_into_contiguous_utc_intervals(
    iso: str, version: int, periods: int, first_start: datetime, last_end: datetime
) -> None:
    parsed = parse(omie_file(iso), day(iso))

    assert (parsed.contract.version, len(parsed.periods)) == (version, periods)
    assert [p.number for p in parsed.periods] == list(range(1, periods + 1))
    assert (parsed.periods[0].start_utc, parsed.periods[-1].end_utc) == (first_start, last_end)
    step = parsed.contract.resolution
    assert all(p.end_utc - p.start_utc == step for p in parsed.periods)
    assert all(a.end_utc == b.start_utc for a, b in pairwise(parsed.periods))


@pytest.mark.parametrize(
    ("iso", "hours", "quarters"),
    [
        ("2024-03-31", 23, 92),
        ("2024-10-27", 25, 100),
        ("2025-09-30", 24, 96),
        ("2026-03-29", 23, 92),
        ("2026-10-25", 25, 100),
        ("2026-11-18", 24, 96),
    ],
)
def test_expected_periods_follow_the_length_of_the_madrid_day(
    iso: str, hours: int, quarters: int
) -> None:
    assert (expected_periods(day(iso), V1), expected_periods(day(iso), V2)) == (hours, quarters)


def test_the_cut_over_picks_the_contract() -> None:
    assert contract_for(CUTOVER - timedelta(days=1)) is V1
    assert contract_for(CUTOVER) is V2
    assert (V1.minutes, V2.minutes) == (60, 15)


def test_column_5_is_portugal_and_column_6_spain() -> None:
    """2025-04-28, the blackout day: the zones decoupled in periods 8, 15 and 16."""
    parsed = parse(omie_file("2025-04-28"), day("2025-04-28"))

    decoupled = {
        p.number: (p.price_es, p.price_pt) for p in parsed.periods if p.price_es != p.price_pt
    }
    assert decoupled == {8: (47.94, 40.7), 15: (-3.0, -1.51), 16: (-1.54, -1.51)}


def redata_spot(iso: str) -> dict[datetime, float]:
    """REData's spot price (``market_prices``, series 600) for a recorded day, by UTC instant."""
    document = json.loads(fixture(f"market_prices-peninsular-{iso}_{iso}.json"))
    [spot] = [series for series in document["included"] if series["id"] == "600"]
    return {
        datetime.fromisoformat(point["datetime"]).astimezone(UTC): point["value"]
        for point in spot["attributes"]["values"]
    }


@pytest.mark.parametrize(
    ("iso", "decoupled"),
    [
        # Hourly OMIE periods; REData repeats each hour's price at four quarter-hour points.
        ("2025-04-28", {8, 15, 16}),
        # Both quarter-hourly.
        ("2025-10-01", {40, 73}),
    ],
)
def test_spain_and_not_portugal_matches_redatas_spot_price(iso: str, decoupled: set[int]) -> None:
    """The phase-4 cross-source check on days the zones decoupled, so that a swap of columns 5
    and 6 fails it: every REData spot point equals the Spanish price of the OMIE period that
    holds it, and differs from the Portuguese one where the zones differ."""
    redata = redata_spot(iso)
    periods = parse(omie_file(iso), day(iso)).periods

    holder = {ts: next(p for p in periods if p.start_utc <= ts < p.end_utc) for ts in redata}

    assert {ts: holder[ts].price_es for ts in redata} == redata
    assert {p.number for p in periods if p.price_es != p.price_pt} == decoupled
    assert all(redata[ts] != p.price_pt for ts, p in holder.items() if p.number in decoupled)
    assert len(redata) == 96  # every quarter-hour of the day, each in one period


@pytest.mark.parametrize("iso", ["2026-03-29", "2026-11-18"])
def test_spain_matches_redatas_spot_price_on_dst_and_recent_days(iso: str) -> None:
    """Instant for instant, on a 92-quarter-hour day and a normal one (the zones did not decouple
    on either, so these say nothing about the column order)."""
    assert {p.start_utc: p.price_es for p in parse(omie_file(iso), day(iso)).periods} == (
        redata_spot(iso)
    )


def test_lf_line_ends_are_accepted() -> None:
    parsed = parse(omie_file("2025-09-30").replace(b"\r\n", b"\n"), day("2025-09-30"))

    assert len(parsed.periods) == 24


def test_a_file_without_its_final_line_end_is_accepted() -> None:
    assert (
        len(parse(omie_file("2025-09-30").removesuffix(b"\r\n"), day("2025-09-30")).periods) == 24
    )


def problems(payload: bytes, iso: str) -> list[str]:
    with pytest.raises(InvalidFile) as caught:
        parse(payload, day(iso))
    return caught.value.problems


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"<!DOCTYPE html>\n<html>",
        b"\xef\xbb\xbfMARGINALPDBC;\r\n*\r\n",
        b"marginalpdbc;\r\n*\r\n",
    ],
)
def test_anything_but_the_header_first_is_refused_at_once(payload: bytes) -> None:
    [problem] = problems(payload, "2025-09-30")

    assert problem.startswith("line 1 is ")
    assert problem.endswith("not the header 'MARGINALPDBC;'")


def test_a_missing_trailer_is_refused() -> None:
    assert problems(build(lines("2025-09-30")[:-2]), "2025-09-30") == [
        "the last line is not the trailer '*'"
    ]


def test_a_missing_period_is_refused() -> None:
    text = lines("2025-10-01")
    del text[50]  # period 50 of 96

    assert problems(build(text), "2025-10-01") == [
        "period 51 where 50 belongs: periods run 1, 2, 3 and so on"
    ]


def test_a_short_day_is_refused() -> None:
    text = lines("2025-10-01")
    del text[-3]  # the last period

    assert problems(build(text), "2025-10-01") == [
        "95 periods, expected 96 (15-minute periods, contract v2)"
    ]


def test_an_hourly_file_after_the_cut_over_is_refused() -> None:
    """2025-09-30's 24 hours relabelled 2025-10-01: a v1 file where v2 is due."""
    hourly = omie_file("2025-09-30").replace(b"2025;09;30;", b"2025;10;01;")

    assert problems(hourly, "2025-10-01") == [
        "24 periods make a v1 file (60-minute periods), but 2025-10-01 is a v2 day "
        "(15-minute periods; the cut-over is 2025-10-01)"
    ]


def test_a_quarter_hourly_file_before_the_cut_over_is_refused() -> None:
    quarters = omie_file("2025-10-01").replace(b"2025;10;01;", b"2025;09;30;")

    assert problems(quarters, "2025-09-30") == [
        "96 periods make a v2 file (15-minute periods), but 2025-09-30 is a v1 day "
        "(60-minute periods; the cut-over is 2025-10-01)"
    ]


def test_a_file_of_another_day_is_refused_line_by_line() -> None:
    found = problems(omie_file("2025-09-30"), "2025-09-29")

    assert found[0] == "line 2: delivery day 2025-09-30, not 2025-09-29"
    assert len(found) == 24


def test_the_message_names_the_file_under_its_version() -> None:
    with pytest.raises(InvalidFile, match=r"^marginalpdbc_20251030\.3: invalid OMIE file: line 1"):
        parse(b"<!DOCTYPE html>", day("2025-10-30"), 3)
    assert parse(omie_file("2025-10-30"), day("2025-10-30"), 3).name == "marginalpdbc_20251030.3"


def test_the_message_shows_five_problems_and_counts_the_rest() -> None:
    with pytest.raises(
        InvalidFile, match=r"marginalpdbc_20250929\.1: invalid OMIE file: .*\(and 19 more\)$"
    ):
        parse(omie_file("2025-09-30"), day("2025-09-29"))


@pytest.mark.parametrize(
    ("line", "problem"),
    [
        ("2025;09;30;1;80,5;80,5;", "'2025;09;30;1;80,5;80,5;' is not YYYY;MM;DD;period;price_PT;"),
        ("2025;09;30;1;80.5;;", "'2025;09;30;1;80.5;;' is not YYYY;MM;DD;period;price_PT;"),
        ("2025;09;30;1;1e3;80;", "'2025;09;30;1;1e3;80;' is not YYYY;MM;DD;period;price_PT;"),
        ("2025;09;30;1;80.5;80.5;81;", "7 fields, expected 6 ('2025;09;30;1;80.5;80.5;81;')"),
        ("2025;09;30;1;80.5;", "5 fields, expected 6"),
        ("2025;09;30;1;80.5;80.5", "'2025;09;30;1;80.5;80.5' does not end with ';'"),
        ("2025;09;30;1;80.5;caf\xe9;", "is not YYYY;MM;DD;period;price_PT;price_ES;"),
        ("2025;09;30;01;80.5;80.5;", "'2025;09;30;01;80.5;80.5;' is not YYYY;MM;DD;period;"),
        ("2025;09;30;0;80.5;80.5;", "'2025;09;30;0;80.5;80.5;' is not YYYY;MM;DD;period;"),
    ],
)
def test_a_line_outside_the_grammar_is_refused(line: str, problem: str) -> None:
    text = lines("2025-09-30")
    text[1] = line

    [found] = problems(build(text), "2025-09-30")

    assert found.startswith("line 2: ")
    assert problem in found


def test_prices_outside_the_harmonised_limits_are_counted_not_refused() -> None:
    text = lines("2025-09-30")
    text[1] = "2025;09;30;1;-500.01;4000.01;"
    text[2] = "2025;09;30;2;-500;4000;"  # the limits themselves are inside

    parsed = parse(build(text), day("2025-09-30"))

    assert outside_limits([parsed]) == {"price_es_eur_mwh": 1, "price_pt_eur_mwh": 1}
    assert outside_limits([parse(omie_file("2025-04-28"), day("2025-04-28"))]) == {}


def test_rows_carry_the_file_and_the_constant_columns() -> None:
    parsed = parse(omie_file("2026-03-29"), day("2026-03-29"))

    first = to_rows(parsed, run_id="r", fetched_at="f", content_sha256="s", landing_path="p")[0]

    assert first == {
        "delivery_date": day("2026-03-29"),
        "period": 1,
        "resolution_minutes": 15,
        "interval_start_utc": utc(2026, 3, 28, 23),
        "interval_end_utc": utc(2026, 3, 28, 23, 15),
        "price_es_eur_mwh": parsed.periods[0].price_es,
        "price_pt_eur_mwh": parsed.periods[0].price_pt,
        "contract_version": 2,
        "source_file": "marginalpdbc_20260329.1",
        "file_version": 1,
        "run_id": "r",
        "fetched_at": "f",
        "content_sha256": "s",
        "landing_path": "p",
    }
