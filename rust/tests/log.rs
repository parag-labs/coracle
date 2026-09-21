use coracle::{Log, LogEntry};

fn commands<'a>(entries: &[LogEntry<&'a str>]) -> Vec<&'a str> {
    entries.iter().map(|e| e.command).collect()
}

#[test]
fn append_and_indices_are_one_based() {
    let mut log: Log<&str> = Log::new();
    assert_eq!(log.last_index(), 0);
    log.append(LogEntry::new(1, "a"));
    log.append(LogEntry::new(1, "b"));
    assert_eq!(log.last_index(), 2);
    assert_eq!(log.get(1).command, "a");
    assert_eq!(log.term_at(2), 1);
}

#[test]
fn index_zero_always_matches() {
    let log: Log<&str> = Log::new();
    assert!(
        log.matches(0, 0),
        "index 0 should always match on an empty log"
    );
}

#[test]
fn matches_requires_same_term() {
    let mut log: Log<&str> = Log::new();
    log.append(LogEntry::new(2, "x"));
    assert!(log.matches(1, 2), "matches(1, 2) should be true");
    assert!(!log.matches(1, 3), "matches(1, 3) should be false");
}

#[test]
fn matches_rejects_out_of_range_index() {
    let mut log: Log<&str> = Log::new();
    log.append(LogEntry::new(1, "a"));
    assert!(
        !log.matches(2, 1),
        "matches past the end of the log should be false"
    );
}

#[test]
fn truncate_drops_conflicting_suffix() {
    let mut log: Log<&str> = Log::new();
    log.append(LogEntry::new(1, "a"));
    log.append(LogEntry::new(1, "b"));
    log.append(LogEntry::new(1, "c"));
    log.truncate_and_append(1, &[LogEntry::new(2, "B"), LogEntry::new(2, "C")]);
    assert_eq!(commands(&log.entries), vec!["a", "B", "C"]);
    assert_eq!(log.term_at(2), 2);
}

#[test]
fn truncate_keeps_matching_prefix_idempotent() {
    let mut log: Log<&str> = Log::new();
    log.append(LogEntry::new(1, "a"));
    log.append(LogEntry::new(1, "b"));
    log.truncate_and_append(0, &[LogEntry::new(1, "a"), LogEntry::new(1, "b")]);
    assert_eq!(commands(&log.entries), vec!["a", "b"]);
}

#[test]
fn truncate_appends_beyond_end() {
    let mut log: Log<&str> = Log::new();
    log.append(LogEntry::new(1, "a"));
    log.truncate_and_append(1, &[LogEntry::new(1, "b"), LogEntry::new(1, "c")]);
    assert_eq!(commands(&log.entries), vec!["a", "b", "c"]);
}

#[test]
fn up_to_date_prefers_higher_term_then_longer_log() {
    let mut log: Log<&str> = Log::new();
    log.append(LogEntry::new(1, "a"));
    log.append(LogEntry::new(2, "b")); // last term 2, last index 2
    assert!(
        log.is_up_to_date(1, 3),
        "higher term should win even if shorter"
    );
    assert!(
        log.is_up_to_date(2, 2),
        "same term, equal length should be up to date"
    );
    assert!(
        !log.is_up_to_date(1, 2),
        "same term, shorter log should lose"
    );
}

#[test]
fn last_term_and_term_at_sentinels() {
    let log: Log<&str> = Log::new();
    assert_eq!(log.last_term(), 0);
    assert_eq!(log.term_at(0), 0, "term at the sentinel index should be 0");
    assert_eq!(
        log.term_at(5),
        0,
        "term at an out-of-range index should be 0"
    );
}

#[test]
fn slice_from_returns_independent_copy() {
    let mut log: Log<&str> = Log::new();
    log.append(LogEntry::new(1, "a"));
    log.append(LogEntry::new(1, "b"));
    let mut suffix = log.slice_from(1);
    assert_eq!(commands(&suffix), vec!["a", "b"]);
    // Mutating the returned vec must not affect the log.
    suffix[0] = LogEntry::new(9, "z");
    assert_eq!(
        log.get(1).command,
        "a",
        "slice_from must return a copy, not an alias"
    );
}
