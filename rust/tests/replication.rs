use coracle::{Cluster, ClusterOptions};

fn elect(c: &mut Cluster<&'static str>) -> usize {
    c.run_until_leader(None)
        .expect("expected a leader to be elected")
}

#[test]
fn proposal_replicates_and_applies_in_order() {
    let mut c: Cluster<&str> = Cluster::new(3);
    let leader = elect(&mut c);
    assert!(
        c.node_mut(leader).propose("x=1"),
        "leader should accept proposal x=1"
    );
    assert!(
        c.node_mut(leader).propose("x=2"),
        "leader should accept proposal x=2"
    );
    c.run(40);

    for id in 0..3 {
        assert_eq!(
            c.committed_commands(id),
            vec!["x=1", "x=2"],
            "node {id} applied the wrong commands",
        );
    }
}

#[test]
fn follower_rejects_client_proposals() {
    let mut c: Cluster<&str> = Cluster::new(3);
    let leader = elect(&mut c);
    let follower = (0..3).find(|&id| id != leader).unwrap();
    assert!(
        !c.node_mut(follower).propose("nope"),
        "a follower must reject client proposals"
    );
}

#[test]
fn commit_index_reaches_a_majority() {
    let mut c: Cluster<&str> = Cluster::new(5);
    let leader = elect(&mut c);
    c.node_mut(leader).propose("a");
    c.run(50);
    let committed_on = (0..5)
        .filter(|&id| c.committed_commands(id) == vec!["a"])
        .count();
    assert!(
        committed_on >= 3,
        "committed on {committed_on} nodes, want a majority (>= 3)"
    );
}

#[test]
fn lagging_follower_catches_up_after_healing() {
    let opts = ClusterOptions {
        heartbeat_interval: 2,
        ..Default::default()
    };
    let mut c: Cluster<&str> = Cluster::with_options(3, opts);
    let leader = elect(&mut c);
    let lagging = (0..3).find(|&id| id != leader).unwrap();

    c.isolate(lagging);
    for cmd in ["c1", "c2", "c3"] {
        c.node_mut(leader).propose(cmd);
    }
    c.run(60);
    assert!(
        c.committed_commands(lagging).is_empty(),
        "isolated follower applied entries it should not have",
    );

    c.heal_all();
    c.run(80);
    assert_eq!(
        c.committed_commands(lagging),
        vec!["c1", "c2", "c3"],
        "the follower should catch up after healing",
    );
}

#[test]
fn minority_partition_cannot_commit() {
    let mut c: Cluster<&str> = Cluster::new(5);
    let leader = elect(&mut c);
    let others: Vec<usize> = (0..5).filter(|&id| id != leader).collect();
    // Cut the leader down to itself plus one follower (a minority of 2 of 5).
    for &nid in &others[1..] {
        c.partition(leader, nid);
    }

    c.node_mut(leader).propose("lonely");
    c.run(60);
    for id in 0..5 {
        assert!(
            !c.committed_commands(id).contains(&"lonely"),
            "node {id} committed 'lonely' from a minority partition",
        );
    }
}

#[test]
fn single_node_accepts_but_cannot_commit_alone() {
    // A lone node elects itself, but commit advancement is driven solely by
    // AppendEntries replies. With no peers it never broadcasts, never gets a
    // reply, and so never commits - the entry sits uncommitted in its log. This
    // mirrors the Python reference exactly.
    let mut c: Cluster<&str> = Cluster::new(1);
    let leader = elect(&mut c);
    assert!(
        c.node_mut(leader).propose("solo"),
        "single-node leader should accept the proposal"
    );
    c.run(20);
    assert!(
        c.committed_commands(0).is_empty(),
        "a single node has no peers, so nothing can ever commit",
    );
    assert_eq!(
        c.node(0).log.last_index(),
        1,
        "the proposal should still sit in the log"
    );
    assert_eq!(
        c.node(0).match_index.get(&0).copied(),
        Some(1),
        "the leader tracks its own match index",
    );
}
