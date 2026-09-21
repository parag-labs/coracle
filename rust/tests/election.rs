use coracle::{Cluster, LogEntry, Message, RaftNode, Role};

fn granted(replies: &[Message<&str>]) -> bool {
    match replies[0] {
        Message::RequestVoteReply { granted, .. } => granted,
        _ => panic!("expected a RequestVoteReply"),
    }
}

#[test]
fn single_node_elects_itself() {
    let mut c: Cluster<&str> = Cluster::new(1);
    let leader = c.run_until_leader(None);
    assert_eq!(leader, Some(0), "single node should elect itself");
}

#[test]
fn cluster_converges_on_one_leader() {
    let mut c: Cluster<&str> = Cluster::new(3);
    let leader = c.run_until_leader(None).expect("expected a leader");
    assert_eq!(c.leaders().len(), 1, "expected exactly one leader");
    let term = c.node(leader).current_term;
    for n in c.nodes() {
        assert_eq!(n.current_term, term, "node {} disagrees on the term", n.id);
    }
}

#[test]
fn five_node_cluster_also_converges() {
    let mut c: Cluster<&str> = Cluster::new(5);
    assert!(c.run_until_leader(None).is_some(), "expected a leader");
    assert_eq!(c.leaders().len(), 1, "expected exactly one leader");
}

#[test]
fn new_leader_emerges_after_the_leader_is_isolated() {
    let mut c: Cluster<&str> = Cluster::new(5);
    let first = c.run_until_leader(None).expect("expected a first leader");
    let first_term = c.node(first).current_term;

    c.isolate(first);
    let mut new_leader = None;
    for _ in 0..300 {
        c.step();
        if let Some(id) = c.leaders().into_iter().find(|&id| id != first) {
            new_leader = Some(id);
            break;
        }
    }
    let new_leader = new_leader.expect("majority should elect a new leader");
    assert_ne!(
        new_leader, first,
        "new leader must differ from the isolated one"
    );
    assert!(
        c.node(new_leader).current_term > first_term,
        "new term should exceed the old term",
    );
}

#[test]
fn term_increments_on_election_timeout() {
    let mut c: Cluster<&str> = Cluster::new(3);
    c.run_until_leader(None);
    let leader = c.leader_id().expect("expected a leader");
    assert!(
        c.node(leader).current_term >= 1,
        "term should be at least 1"
    );
}

#[test]
fn stale_candidate_is_denied() {
    let mut voter: RaftNode<&str> = RaftNode::new(0, &[0, 1], 10, 3);
    voter.current_term = 5;
    voter.log.append(LogEntry::new(5, "committed"));

    let stale = Message::RequestVote {
        src: 1,
        dst: 0,
        term: 6,
        last_log_index: 0,
        last_log_term: 0,
    };
    let replies = voter.handle(&stale);
    assert!(
        !granted(&replies),
        "a stale candidate must not get the vote"
    );
    assert_eq!(
        voter.role,
        Role::Follower,
        "voter should adopt the higher term as a follower"
    );
    assert_eq!(
        voter.current_term, 6,
        "voter should advance to the candidate's term"
    );
}

#[test]
fn up_to_date_candidate_is_granted_the_vote() {
    let mut voter: RaftNode<&str> = RaftNode::new(0, &[0, 1], 10, 3);
    voter.current_term = 2;
    voter.log.append(LogEntry::new(1, "a"));

    let req = Message::RequestVote {
        src: 1,
        dst: 0,
        term: 3,
        last_log_index: 1,
        last_log_term: 1,
    };
    assert!(
        granted(&voter.handle(&req)),
        "an up-to-date candidate at a higher term should get the vote",
    );
}

#[test]
fn vote_is_not_granted_twice_to_different_candidates() {
    let mut voter: RaftNode<&str> = RaftNode::new(0, &[0, 1, 2], 10, 3);

    let first = Message::RequestVote {
        src: 1,
        dst: 0,
        term: 1,
        last_log_index: 0,
        last_log_term: 0,
    };
    assert!(
        granted(&voter.handle(&first)),
        "first candidate at a new term should get the vote"
    );

    let second = Message::RequestVote {
        src: 2,
        dst: 0,
        term: 1,
        last_log_index: 0,
        last_log_term: 0,
    };
    assert!(
        !granted(&voter.handle(&second)),
        "a second candidate in the same term must be denied",
    );
}
