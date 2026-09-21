//! The chaos suite hammers the cluster with a lossy, out-of-order, duplicating
//! network and rolling partitions, and asserts Raft's safety invariants never
//! break: at most one leader per term, and no two nodes ever apply a different
//! command at the same log index. Liveness is checked separately, only after the
//! network is fully healed.

use std::collections::HashMap;

use coracle::{Cluster, ClusterOptions, Rng, Role};

fn check_election_safety<C: Clone>(c: &Cluster<C>) {
    let mut by_term: HashMap<u64, usize> = HashMap::new();
    for n in c.nodes() {
        if n.role == Role::Leader {
            *by_term.entry(n.current_term).or_insert(0) += 1;
        }
    }
    for (term, count) in by_term {
        assert!(count <= 1, "two leaders in term {term}");
    }
}

fn check_logs_consistent<C: Clone + PartialEq + std::fmt::Debug>(
    c: &Cluster<C>,
    committed: &mut HashMap<usize, C>,
) {
    for n in c.nodes() {
        for (offset, entry) in n.applied.iter().enumerate() {
            let index = offset + 1;
            let prior = committed
                .entry(index)
                .or_insert_with(|| entry.command.clone());
            assert!(
                *prior == entry.command,
                "index {index} diverged: {prior:?} vs {:?}",
                entry.command,
            );
        }
    }
}

fn drain_and_assert<C: Clone + PartialEq + std::fmt::Debug>(
    c: &mut Cluster<C>,
    committed: &mut HashMap<usize, C>,
    steps: usize,
) {
    for _ in 0..steps {
        c.step();
        check_election_safety(c);
        check_logs_consistent(c, committed);
    }
}

/// Returns two distinct values in `[0, n)`.
fn sample_two(r: &mut Rng, n: usize) -> (usize, usize) {
    let a = r.below(n);
    let mut b = r.below(n - 1);
    if b >= a {
        b += 1;
    }
    (a, b)
}

#[test]
fn safety_holds_under_message_drops() {
    let mut committed: HashMap<usize, String> = HashMap::new();
    let opts = ClusterOptions {
        seed: Some(1),
        drop_prob: 0.15,
        ..Default::default()
    };
    let mut c: Cluster<String> = Cluster::with_options(5, opts);
    let leader = c
        .run_until_leader(Some(300))
        .expect("expected a leader even under message drops");
    for i in 0..10 {
        let l = c.leader_id().unwrap_or(leader);
        c.node_mut(l).propose(format!("cmd-{i}"));
        drain_and_assert(&mut c, &mut committed, 8);
    }
    drain_and_assert(&mut c, &mut committed, 200);
}

#[test]
fn safety_holds_under_duplication_and_reorder() {
    let mut committed: HashMap<usize, String> = HashMap::new();
    let opts = ClusterOptions {
        seed: Some(7),
        duplicate_prob: 0.25,
        reorder: true,
        ..Default::default()
    };
    let mut c: Cluster<String> = Cluster::with_options(5, opts);
    let leader = c
        .run_until_leader(Some(300))
        .expect("expected a leader even under duplication and reordering");
    for i in 0..15 {
        let l = c.leader_id().unwrap_or(leader);
        c.node_mut(l).propose(format!("x{i}"));
        drain_and_assert(&mut c, &mut committed, 6);
    }
    drain_and_assert(&mut c, &mut committed, 150);
}

#[test]
fn rolling_partitions_keep_safety_and_eventually_make_progress() {
    let mut committed: HashMap<usize, String> = HashMap::new();
    let mut r = Rng::new(42);
    let opts = ClusterOptions {
        seed: Some(42),
        drop_prob: 0.05,
        ..Default::default()
    };
    let mut c: Cluster<String> = Cluster::with_options(5, opts);
    c.run_until_leader(Some(300));

    let mut proposed = 0;
    for _ in 0..40 {
        let (a, b) = sample_two(&mut r, 5);
        if r.next_f64() < 0.5 {
            c.partition(a, b);
        } else {
            c.heal(a, b);
        }
        if let Some(leader) = c.leader_id() {
            c.node_mut(leader).propose(format!("v{proposed}"));
            proposed += 1;
        }
        drain_and_assert(&mut c, &mut committed, 6);
    }

    c.heal_all();
    drain_and_assert(&mut c, &mut committed, 400);

    let leader = c
        .run_until_leader(Some(200))
        .expect("cluster should elect a leader once healed");
    let target = c.committed_commands(leader);
    let mut agree = 0;
    for id in 0..5 {
        let applied = c.committed_commands(id);
        if applied.len() < target.len() {
            continue;
        }
        if applied[..target.len()] == target[..] {
            agree += 1;
        }
    }
    assert!(agree >= 3, "only {agree}/5 nodes converged");
    assert!(!target.is_empty(), "cluster made no progress at all");
}

#[test]
fn soak_many_random_schedules() {
    let drop_choices = [0.0, 0.1, 0.2];
    let dup_choices = [0.0, 0.15];
    for s in 0..25u64 {
        let mut committed: HashMap<usize, i64> = HashMap::new();
        let mut r = Rng::new(s);
        let drop = drop_choices[r.below(drop_choices.len())];
        let dup = dup_choices[r.below(dup_choices.len())];
        let reorder = r.next_f64() < 0.5;
        let opts = ClusterOptions {
            seed: Some(s),
            drop_prob: drop,
            duplicate_prob: dup,
            reorder,
            ..Default::default()
        };
        let mut c: Cluster<i64> = Cluster::with_options(5, opts);
        c.run_until_leader(Some(300));
        for _ in 0..30 {
            if let Some(leader) = c.leader_id() {
                c.node_mut(leader).propose(r.randint(0, 1_000_000));
            }
            let steps = r.randint(3, 9) as usize;
            drain_and_assert(&mut c, &mut committed, steps);
        }
    }
}
