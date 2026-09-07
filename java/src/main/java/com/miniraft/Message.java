// The RPC messages exchanged between nodes. These are plain data; the node returns
// them from its handlers and the cluster delivers them.

package com.miniraft;

import java.util.ArrayList;
import java.util.List;

public sealed interface Message
        permits Message.RequestVote, Message.RequestVoteReply,
                Message.AppendEntries, Message.AppendEntriesReply {

    int src();
    int dst();
    int term();

    record RequestVote(int src, int dst, int term, int lastLogIndex, int lastLogTerm)
            implements Message {}

    record RequestVoteReply(int src, int dst, int term, boolean granted)
            implements Message {}

    record AppendEntries(int src, int dst, int term, int prevLogIndex, int prevLogTerm,
                         List<Log.Entry> entries, int leaderCommit) implements Message {
        public AppendEntries {
            entries = entries == null ? new ArrayList<>() : entries;
        }
    }

    record AppendEntriesReply(int src, int dst, int term, boolean success, int matchIndex)
            implements Message {}
}
