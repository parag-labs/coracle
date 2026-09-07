// Log mechanics: matching, conflict truncation, and the election restriction.

using Xunit;

namespace Coracle.Tests;

public class LogTests
{
    [Fact]
    public void AppendAndIndicesAreOneBased()
    {
        var log = new Log();
        Assert.Equal(0, log.LastIndex());
        log.Append(new LogEntry(1, "a"));
        log.Append(new LogEntry(1, "b"));
        Assert.Equal(2, log.LastIndex());
        Assert.Equal("a", log.Get(1).Command);
        Assert.Equal(1, log.TermAt(2));
    }

    [Fact]
    public void IndexZeroAlwaysMatches() => Assert.True(new Log().Matches(0, 0));

    [Fact]
    public void MatchesRequiresSameTerm()
    {
        var log = new Log();
        log.Append(new LogEntry(2, "x"));
        Assert.True(log.Matches(1, 2));
        Assert.False(log.Matches(1, 3));
    }

    [Fact]
    public void TruncateDropsConflictingSuffix()
    {
        var log = new Log();
        log.Append(new LogEntry(1, "a"));
        log.Append(new LogEntry(1, "b"));
        log.Append(new LogEntry(1, "c"));
        // A new leader at term 2 overwrites from index 2 onward.
        log.TruncateAndAppend(1, new List<LogEntry> { new(2, "B"), new(2, "C") });
        Assert.Equal(new object[] { "a", "B", "C" }, log.Entries.Select(e => e.Command));
        Assert.Equal(2, log.TermAt(2));
    }

    [Fact]
    public void TruncateKeepsMatchingPrefixIdempotent()
    {
        var log = new Log();
        log.Append(new LogEntry(1, "a"));
        log.Append(new LogEntry(1, "b"));
        // Re-delivering the same entries must not duplicate or rewrite them.
        log.TruncateAndAppend(0, new List<LogEntry> { new(1, "a"), new(1, "b") });
        Assert.Equal(new object[] { "a", "b" }, log.Entries.Select(e => e.Command));
    }

    [Fact]
    public void UpToDatePrefersHigherTermThenLongerLog()
    {
        var log = new Log();
        log.Append(new LogEntry(1, "a"));
        log.Append(new LogEntry(2, "b")); // last term 2, last index 2
        Assert.True(log.IsUpToDate(1, 3));  // higher term wins even if shorter
        Assert.True(log.IsUpToDate(2, 2));  // same term, equal length
        Assert.False(log.IsUpToDate(1, 2)); // same term, shorter loses
    }
}
