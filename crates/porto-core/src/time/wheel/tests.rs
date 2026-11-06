//! Wheel tests: fixed cases, the clamp and re-arm path, and a proptest against a `BTreeMap`
//! model (`52-testing-strategy.md` item 1). They run under Miri too (`cargo +nightly miri test
//! -p porto-core time::wheel`).

use std::collections::BTreeMap;
use std::pin::Pin;
use std::ptr::NonNull;

use proptest::prelude::*;

use super::{MAX_SPAN, TimerEntry, Wheel};

/// Entries for a test, pinned on the heap and addressed by index.
struct Entries(Vec<Pin<Box<TimerEntry>>>);

impl Entries {
    fn new(count: usize) -> Self {
        Self((0..count).map(|_| Box::pin(TimerEntry::new())).collect())
    }

    fn ptr(&self, index: usize) -> NonNull<TimerEntry> {
        NonNull::from(&*self.0[index])
    }

    fn index_of(&self, entry: NonNull<TimerEntry>) -> usize {
        (0..self.0.len())
            .find(|&index| self.ptr(index) == entry)
            .expect("the wheel returned an entry it was not given")
    }

    fn is_linked(&self, index: usize) -> bool {
        // SAFETY: the test owns the wheel and the entries; nothing else touches them.
        unsafe { TimerEntry::is_linked(self.ptr(index)) }
    }

    fn deadline_tick(&self, index: usize) -> u64 {
        // SAFETY: as above.
        unsafe { (*TimerEntry::state(self.ptr(index))).deadline_tick }
    }
}

/// Inserts entry `index` to fire at `tick`.
fn insert(wheel: &mut Wheel, entries: &Entries, index: usize, tick: u64) {
    assert!(!entries.is_linked(index));
    // SAFETY: the entry is unlinked, and outlives the wheel's use of it (the test drains or
    // removes every entry before `entries` is dropped).
    unsafe { wheel.insert(entries.ptr(index), tick) };
}

fn remove(wheel: &mut Wheel, entries: &Entries, index: usize) {
    assert!(entries.is_linked(index));
    // SAFETY: linked in `wheel`.
    unsafe { wheel.remove(entries.ptr(index)) };
}

/// Advances the wheel to `now`: the indexes of the entries that fired, in order.
fn advance(wheel: &mut Wheel, entries: &Entries, now: u64) -> Vec<usize> {
    let mut fired = Vec::new();
    while let Some(entry) = wheel.pop_expired(now) {
        let index = entries.index_of(entry);
        assert!(!entries.is_linked(index), "a popped entry is unlinked");
        assert!(
            entries.deadline_tick(index) <= now,
            "entry {index} fired at {now}, before its tick {}",
            entries.deadline_tick(index)
        );
        fired.push(index);
    }
    assert_eq!(wheel.elapsed(), now.max(wheel.elapsed()));
    fired
}

fn drain(wheel: &mut Wheel) {
    while wheel.pop_any().is_some() {}
}

#[test]
fn an_entry_fires_at_its_tick_and_not_before() {
    let entries = Entries::new(1);
    let mut wheel = Wheel::new();
    insert(&mut wheel, &entries, 0, 100);
    assert_eq!(wheel.next_deadline_tick(), Some(100));
    assert!(advance(&mut wheel, &entries, 99).is_empty());
    assert_eq!(advance(&mut wheel, &entries, 100), [0]);
    assert!(wheel.is_empty());
    assert_eq!(wheel.next_deadline_tick(), None);
}

#[test]
fn entries_fire_in_tick_order_and_ties_in_insertion_order() {
    let entries = Entries::new(6);
    let mut wheel = Wheel::new();
    // Ticks across levels 0, 1 and 2, with ties on 70 and 5_000.
    for (index, tick) in [(0, 5_000), (1, 70), (2, 3), (3, 70), (4, 5_000), (5, 64)] {
        insert(&mut wheel, &entries, index, tick);
    }
    assert_eq!(wheel.len(), 6);
    assert_eq!(advance(&mut wheel, &entries, 10_000), [2, 5, 1, 3, 0, 4]);
}

#[test]
fn a_cancelled_entry_never_fires() {
    let entries = Entries::new(3);
    let mut wheel = Wheel::new();
    insert(&mut wheel, &entries, 0, 10);
    insert(&mut wheel, &entries, 1, 10);
    insert(&mut wheel, &entries, 2, 10);
    // Middle, then head, then the last one: every unlink case of a slot list.
    remove(&mut wheel, &entries, 1);
    remove(&mut wheel, &entries, 0);
    assert_eq!(advance(&mut wheel, &entries, 10), [2]);
    insert(&mut wheel, &entries, 0, 20);
    remove(&mut wheel, &entries, 0);
    assert!(wheel.is_empty());
    assert_eq!(wheel.next_deadline_tick(), None);
    assert!(advance(&mut wheel, &entries, 100).is_empty());
}

#[test]
fn a_past_tick_fires_on_the_next_advance() {
    let entries = Entries::new(1);
    let mut wheel = Wheel::new();
    assert!(advance(&mut wheel, &entries, 1_000).is_empty());
    insert(&mut wheel, &entries, 0, 10);
    assert_eq!(wheel.next_deadline_tick(), Some(1_000));
    assert_eq!(advance(&mut wheel, &entries, 1_000), [0]);
}

#[test]
fn the_next_deadline_is_the_earliest_entry_not_the_cascade() {
    let entries = Entries::new(2);
    let mut wheel = Wheel::new();
    // Both on level 2 (64^2 = 4096 ticks per slot), in the same slot.
    insert(&mut wheel, &entries, 0, 9_000);
    insert(&mut wheel, &entries, 1, 8_500);
    assert_eq!(wheel.next_deadline_tick(), Some(8_500));
    assert!(advance(&mut wheel, &entries, 8_499).is_empty());
    assert_eq!(wheel.next_deadline_tick(), Some(8_500));
    assert_eq!(advance(&mut wheel, &entries, 8_500), [1]);
    assert_eq!(advance(&mut wheel, &entries, 9_000), [0]);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "100k entries and a wall-clock bound: too slow under miri"
)]
fn the_next_deadline_of_a_crowded_slot_is_found_without_walking_it() {
    // porto-szb.95: 100k sleeps of about the same long duration share one level-3 slot (64^3
    // ticks); the park's next-deadline query reads the slot's cached bound instead of walking
    // the list. Timed against the walk it replaced: 100k pointer hops per park.
    const COUNT: usize = 100_000;
    let entries = Entries::new(COUNT);
    let mut wheel = Wheel::new();
    let base = 1 << 19;
    for index in 0..COUNT {
        insert(&mut wheel, &entries, index, base + 1_000 + index as u64);
    }
    assert_eq!(wheel.next_deadline_tick(), Some(base + 1_000));
    let started = std::time::Instant::now();
    for _ in 0..10_000 {
        assert_eq!(
            std::hint::black_box(&wheel).next_deadline_tick(),
            Some(base + 1_000)
        );
    }
    // 10k queries in well under the time of one 100k-entry walk each (seconds in all).
    assert!(
        started.elapsed() < std::time::Duration::from_millis(500),
        "{:?} for 10k queries",
        started.elapsed()
    );
    drain(&mut wheel);
}

#[test]
fn cancelling_the_earliest_entry_of_a_slot_costs_at_most_one_early_wake() {
    let entries = Entries::new(3);
    let mut wheel = Wheel::new();
    // All three in one level-2 slot (4096 ticks), the earliest cancelled.
    insert(&mut wheel, &entries, 0, 8_300);
    insert(&mut wheel, &entries, 1, 9_000);
    insert(&mut wheel, &entries, 2, 8_700);
    remove(&mut wheel, &entries, 0);
    // The bound is stale: early, never late.
    let stale = wheel.next_deadline_tick().unwrap();
    assert!((8_192..=8_700).contains(&stale), "{stale}");
    // The early wake cascades the slot and fires nothing; the bound is exact from then on.
    assert!(advance(&mut wheel, &entries, stale).is_empty());
    assert_eq!(wheel.next_deadline_tick(), Some(8_700));
    assert_eq!(advance(&mut wheel, &entries, 8_700), [2]);
    assert_eq!(wheel.next_deadline_tick(), Some(9_000));
    // A slot that empties forgets its bound.
    remove(&mut wheel, &entries, 1);
    assert_eq!(wheel.next_deadline_tick(), None);
    insert(&mut wheel, &entries, 1, 20_000);
    assert_eq!(wheel.next_deadline_tick(), Some(20_000));
    drain(&mut wheel);
}

#[test]
fn a_tick_beyond_the_span_is_clamped_and_re_armed() {
    let entries = Entries::new(2);
    let mut wheel = Wheel::new();
    let far = 3 * MAX_SPAN + 12_345;
    insert(&mut wheel, &entries, 0, far);
    // A neighbor within the span keeps firing normally.
    insert(&mut wheel, &entries, 1, MAX_SPAN - 1);
    let mut wakes = 0;
    let now = loop {
        // Parking until the next deadline never oversleeps the far entry...
        let now = wheel.next_deadline_tick().unwrap();
        assert!(now <= far);
        let fired = advance(&mut wheel, &entries, now);
        if fired.contains(&0) {
            break now;
        }
        // ...and the clamped entry stays linked, with its real tick, until it is reached.
        assert!(entries.is_linked(0));
        assert_eq!(entries.deadline_tick(0), far);
        wakes += 1;
        assert!(wakes < 20, "re-armed too often: {wakes} wakes");
    };
    assert_eq!(now, far);
    assert!(wheel.is_empty());
    // About one wake per span to re-arm, plus the neighbor.
    assert!(wakes >= 3, "{wakes} wakes");
}

#[test]
fn a_huge_tick_does_not_overflow() {
    let entries = Entries::new(1);
    let mut wheel = Wheel::new();
    insert(&mut wheel, &entries, 0, u64::MAX);
    let next = wheel.next_deadline_tick().unwrap();
    assert!(next < MAX_SPAN);
    assert!(advance(&mut wheel, &entries, next).is_empty());
    assert!(entries.is_linked(0));
    drain(&mut wheel);
    assert!(!entries.is_linked(0));
}

#[test]
fn inserting_while_firing_at_the_current_tick_fires_in_the_same_pass() {
    let entries = Entries::new(3);
    let mut wheel = Wheel::new();
    insert(&mut wheel, &entries, 0, 50);
    insert(&mut wheel, &entries, 1, 50);
    let first = wheel.pop_expired(50).unwrap();
    assert_eq!(entries.index_of(first), 0);
    // A wake inserting a timer for the tick being fired: it joins the tail of the slot.
    insert(&mut wheel, &entries, 2, 50);
    assert_eq!(advance(&mut wheel, &entries, 50), [1, 2]);
}

#[test]
fn pop_any_drains_every_level() {
    let entries = Entries::new(4);
    let mut wheel = Wheel::new();
    for (index, tick) in [(0, 1), (1, 100), (2, 1 << 20), (3, u64::MAX)] {
        insert(&mut wheel, &entries, index, tick);
    }
    let mut drained = Vec::new();
    while let Some(entry) = wheel.pop_any() {
        drained.push(entries.index_of(entry));
    }
    drained.sort_unstable();
    assert_eq!(drained, [0, 1, 2, 3]);
    assert!(wheel.is_empty());
    assert_eq!(wheel.next_deadline_tick(), None);
}

#[test]
fn an_entry_is_48_bytes() {
    assert_eq!(size_of::<TimerEntry>(), 48);
}

/// A step of the model test.
#[derive(Debug, Clone)]
enum Op {
    /// Arm entry `index` (re-arm if armed) `delay` ticks after the wheel's time.
    Insert { index: usize, delay: u64 },
    /// Cancel entry `index` if armed.
    Cancel { index: usize },
    /// Move time forward by `by` ticks and fire what is due.
    Advance { by: u64 },
    /// Advance to the wheel's next deadline (what a lane's park does).
    AdvanceToNext,
}

const MODEL_ENTRIES: usize = 12;

/// Delays and steps at every scale: within a level-0 slot, across levels, beyond the span.
fn ticks() -> impl Strategy<Value = u64> {
    prop_oneof![
        4 => 0..70u64,
        3 => 0..5_000u64,
        2 => 0..(1u64 << 24),
        1 => 0..(4 * MAX_SPAN),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0..MODEL_ENTRIES, ticks()).prop_map(|(index, delay)| Op::Insert { index, delay }),
        2 => (0..MODEL_ENTRIES).prop_map(|index| Op::Cancel { index }),
        3 => ticks().prop_map(|by| Op::Advance { by }),
        1 => Just(Op::AdvanceToNext),
    ]
}

/// The model: armed entries by (tick, insertion sequence).
#[derive(Default)]
struct Model {
    armed: BTreeMap<(u64, u64), usize>,
    sequence: u64,
}

impl Model {
    fn insert(&mut self, index: usize, tick: u64) {
        self.cancel(index);
        self.armed.insert((tick, self.sequence), index);
        self.sequence += 1;
    }

    fn cancel(&mut self, index: usize) {
        self.armed.retain(|_, armed| *armed != index);
    }

    fn is_armed(&self, index: usize) -> bool {
        self.armed.values().any(|&armed| armed == index)
    }

    /// Fires everything due at `now`, in order.
    fn advance(&mut self, now: u64) -> Vec<usize> {
        let later = self.armed.split_off(&(now + 1, 0));
        std::mem::replace(&mut self.armed, later)
            .into_values()
            .collect()
    }

    fn earliest(&self) -> Option<u64> {
        self.armed.keys().next().map(|&(tick, _)| tick)
    }
}

fn run_model(ops: &[Op]) -> Result<(), TestCaseError> {
    let entries = Entries::new(MODEL_ENTRIES);
    let mut wheel = Wheel::new();
    let mut model = Model::default();
    let mut now = 0;
    // Same-tick entries fire in insertion order, except that an entry re-armed from beyond
    // the span joins its tick's slot when it gets there: only the tick order is checked then.
    let mut clamped = false;
    // A removal may leave a slot's cached lower bound below its earliest entry: the next
    // deadline may then be early (never late) until the slot cascades or empties.
    let mut removed = false;
    for op in ops {
        match *op {
            Op::Insert { index, delay } => {
                if entries.is_linked(index) {
                    remove(&mut wheel, &entries, index);
                    removed = true;
                }
                let tick = now + delay;
                clamped |= delay >= MAX_SPAN - 1;
                insert(&mut wheel, &entries, index, tick);
                model.insert(index, tick);
            }
            Op::Cancel { index } => {
                if entries.is_linked(index) {
                    remove(&mut wheel, &entries, index);
                    removed = true;
                }
                model.cancel(index);
            }
            Op::Advance { by } => {
                now += by;
                check_fired(
                    &entries,
                    advance(&mut wheel, &entries, now),
                    model.advance(now),
                    clamped,
                )?;
            }
            Op::AdvanceToNext => {
                if let Some(next) = wheel.next_deadline_tick() {
                    now = now.max(next);
                    let fired = advance(&mut wheel, &entries, now);
                    check_fired(&entries, fired, model.advance(now), clamped)?;
                }
            }
        }
        prop_assert_eq!(wheel.len(), model.armed.len());
        for index in 0..MODEL_ENTRIES {
            prop_assert_eq!(entries.is_linked(index), model.is_armed(index));
        }
        // The next deadline is never after the earliest entry (a park would oversleep it), and
        // is exact unless an entry went beyond the span (then it may be a re-arm point).
        match (wheel.next_deadline_tick(), model.earliest()) {
            (None, None) => {}
            (Some(next), Some(earliest)) => {
                prop_assert!(
                    next <= earliest.max(now),
                    "next {next}, earliest {earliest}"
                );
                prop_assert!(next >= wheel.elapsed(), "next {next} is in the past");
                if !clamped && !removed {
                    prop_assert_eq!(next, earliest.max(wheel.elapsed()));
                }
            }
            (next, earliest) => {
                prop_assert!(false, "next {next:?} but earliest {earliest:?}");
            }
        }
        if wheel.is_empty() {
            removed = false;
        }
    }
    drain(&mut wheel);
    Ok(())
}

fn check_fired(
    entries: &Entries,
    mut fired: Vec<usize>,
    mut expected: Vec<usize>,
    clamped: bool,
) -> Result<(), TestCaseError> {
    if clamped {
        let ticks: Vec<u64> = fired
            .iter()
            .map(|&index| entries.deadline_tick(index))
            .collect();
        prop_assert!(ticks.is_sorted(), "fired out of tick order: {ticks:?}");
        fired.sort_unstable();
        expected.sort_unstable();
    }
    prop_assert_eq!(fired, expected);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: if cfg!(miri) { 4 } else { 1_000 },
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    #[test]
    fn the_wheel_matches_a_btreemap_model(ops in proptest::collection::vec(op(), 1..120)) {
        run_model(&ops)?;
    }
}
