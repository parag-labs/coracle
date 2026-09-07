"""Log mechanics: matching, conflict truncation, and the election restriction."""

from __future__ import annotations

from raft.log import Log, LogEntry


def test_append_and_indices_are_one_based():
    log = Log()
    assert log.last_index() == 0
    log.append(LogEntry(1, "a"))
    log.append(LogEntry(1, "b"))
    assert log.last_index() == 2
    assert log.get(1).command == "a"
    assert log.term_at(2) == 1


def test_index_zero_always_matches():
    assert Log().matches(0, 0) is True


def test_matches_requires_same_term():
    log = Log()
    log.append(LogEntry(2, "x"))
    assert log.matches(1, 2) is True
    assert log.matches(1, 3) is False


def test_truncate_drops_conflicting_suffix():
    log = Log()
    log.append(LogEntry(1, "a"))
    log.append(LogEntry(1, "b"))
    log.append(LogEntry(1, "c"))
    # A new leader at term 2 overwrites from index 2 onward.
    log.truncate_and_append(1, [LogEntry(2, "B"), LogEntry(2, "C")])
    assert [e.command for e in log.entries] == ["a", "B", "C"]
    assert log.term_at(2) == 2


def test_truncate_keeps_matching_prefix_idempotent():
    log = Log()
    log.append(LogEntry(1, "a"))
    log.append(LogEntry(1, "b"))
    # Re-delivering the same entries must not duplicate or rewrite them.
    log.truncate_and_append(0, [LogEntry(1, "a"), LogEntry(1, "b")])
    assert [e.command for e in log.entries] == ["a", "b"]


def test_up_to_date_prefers_higher_term_then_longer_log():
    log = Log()
    log.append(LogEntry(1, "a"))
    log.append(LogEntry(2, "b"))  # last term 2, last index 2
    assert log.is_up_to_date(1, 3) is True  # higher term wins even if shorter
    assert log.is_up_to_date(2, 2) is True  # same term, equal length
    assert log.is_up_to_date(1, 2) is False  # same term, shorter loses
