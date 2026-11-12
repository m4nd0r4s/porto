"""The semantic suite of 16-sync-primitives-and-channels.md (an M1 exit criterion).

One canonical test per rule: SY1-SY39 of §4 (the chapter has no numbered rules of its own, so §4
gives them IDs; they start with ``SY`` because ``S1``-``S7`` belong to the goals of 01), and a test
claims its rule in its name (``test_SY4_...``, 52 §3), which ``scripts/rule_matrix.py --check 16``
holds CI to. Each canonical test is the rule in its simplest form. ``RULE_INDEX`` below names the
deeper tests of every rule that live in the other ``tests/python`` files (edge cases, threads,
cancellation races, leaks), and ``test_the_rule_index_names_tests_that_exist`` keeps the table
honest. The loom models of the same rules (grant against cancel, close races) are in
``crates/porto-core``, run by ``cargo test`` with ``--cfg loom`` (51 §2).
"""

import _thread
import ast
import math
import signal
import threading
from pathlib import Path
from typing import Any

import pytest

import porto
from porto import _core

TESTS = Path(__file__).resolve().parent.parent

#: How long a test waits for a thread on a loaded CI machine before it calls it a hang.
LATE = 10.0

#: The deeper tests of each rule, in the other files of tests/python: "file::test" (a prefix).
RULE_INDEX: dict[str, tuple[str, ...]] = {
    "SY1": (
        "test_sync_lock.py::test_try_acquire_and_release",
        "test_sync_lock.py::test_the_lock_is_not_reentrant",
        "test_sync_lock.py::test_a_release_by_another_task_breaks_exclusion",
    ),
    "SY2": (
        "test_sync_lock.py::test_mutex_is_subscriptable_and_gives_its_value_through_a_guard",
        "test_sync_lock.py::test_a_mutex_guard_stops_working_after_its_block",
        "test_sync_lock.py::test_mutex_guard_set_takes_one_value",
    ),
    "SY3": (
        "test_sync_rwlock.py::test_readers_share_and_a_writer_excludes_them",
        "test_sync_rwlock.py::test_a_waiting_writer_blocks_new_readers",
        "test_sync_rwlock.py::test_a_cancelled_writer_unblocks_the_readers_queued_behind_it",
        "test_sync_rwlock.py::test_waiters_are_served_in_arrival_order_with_readers_together",
        "test_sync_rwlock.py::test_releasing_what_is_not_held_raises",
        "test_sync_rwlock.py::test_a_mixed_load_of_1000_iterations_starves_nobody",
    ),
    "SY4": (
        "test_sync_semaphore.py::test_a_large_request_blocks_smaller_ones_behind_it",
        "test_sync_semaphore.py::test_releasing_more_than_was_acquired_raises",
        "test_sync_semaphore.py::test_a_semaphore_needs_at_least_one_permit",
        "test_sync_semaphore.py::test_a_count_outside_one_to_the_value_raises",
    ),
    "SY5": (
        "test_sync_semaphore.py::test_a_limiter_lends_tokens_to_tasks",
        "test_sync_semaphore.py::test_a_task_borrowing_twice_raises_as_anyio_does",
        "test_sync_semaphore.py::test_releasing_a_token_the_borrower_does_not_hold_raises",
        "test_sync_semaphore.py::test_tokens_are_borrowed_per_borrower_object",
        "test_sync_semaphore.py::test_raising_the_total_grants_waiting_tasks",
        "test_sync_semaphore.py::test_lowering_the_total_takes_tokens_back_as_they_are_released",
        "test_sync_semaphore.py::test_an_unlimited_limiter_reports_infinity",
        "test_sync_semaphore.py::test_a_bad_total_says_what_it_takes",
    ),
    "SY6": (
        "test_sync_event_notify.py::test_event_starts_unset_and_stays_set",
        "test_sync_event_notify.py::test_event_wait_after_set_returns_without_suspending",
        "test_sync_event_notify.py::test_event_set_releases_every_waiter",
    ),
    "SY7": (
        "test_sync_event_notify.py::test_notify_one_stores_one_permit_consumed_once",
        "test_sync_event_notify.py::test_notify_one_wakes_waiters_in_arrival_order",
        "test_sync_event_notify.py::test_notify_all_wakes_only_current_waiters",
        "test_sync_event_notify.py::test_a_permit_is_not_lost_when_the_run_ends_after_notified",
    ),
    "SY8": (
        "test_sync_condition_barrier.py::test_wait_releases_the_lock_and_takes_it_again_once_notified",
        "test_sync_condition_barrier.py::test_wait_takes_the_lock_again_even_when_cancelled",
        "test_sync_condition_barrier.py::test_wait_for_rechecks_its_predicate_after_each_notification",
        "test_sync_condition_barrier.py::test_wait_for_a_true_predicate_does_not_suspend",
        "test_sync_condition_barrier.py::test_waiting_or_notifying_without_the_lock_raises",
        "test_sync_condition_barrier.py::test_a_condition_takes_a_porto_lock_only",
    ),
    "SY9": (
        "test_sync_condition_barrier.py::test_notify_wakes_the_longest_waiting_first",
        "test_sync_condition_barrier.py::test_a_notification_whose_waiter_never_ran_leaks_nothing",
        "test_sync_condition_barrier.py::test_an_interrupted_waiter_leaves_the_queue_and_takes_the_lock_again",
    ),
    "SY10": (
        "test_sync_condition_barrier.py::test_a_waiter_freed_while_another_task_holds_the_lock_leaves_it_held",
        "test_sync_condition_barrier.py::test_a_waiter_freed_while_relocking_leaves_the_holders_lock_held",
        "test_sync_condition_barrier.py::test_a_waiter_freed_with_an_unobserved_relock_grant_passes_it_on_cleanly",
    ),
    "SY11": (
        "test_sync_condition_barrier.py::test_exactly_one_leader_per_round_and_the_barrier_is_reusable",
        "test_sync_condition_barrier.py::test_nobody_goes_on_before_the_last_arrival",
        "test_sync_condition_barrier.py::test_a_cancelled_waiter_leaves_its_round",
        "test_sync_condition_barrier.py::test_a_barrier_needs_a_party",
    ),
    "SY12": (
        "test_sync_lock.py::test_waiters_acquire_in_arrival_order_under_contention",
        "test_sync_lock.py::test_a_released_lock_is_handed_over_so_nobody_barges",
        "test_sync_lock.py::test_mutex_waiters_get_it_in_arrival_order",
        "test_sync_semaphore.py::test_waiters_are_granted_in_arrival_order",
        "test_sync_event_notify.py::test_notify_one_wakes_waiters_in_arrival_order",
    ),
    "SY13": (
        "test_sync_lock.py::test_an_interrupted_waiter_does_not_leak_the_lock",
        "test_sync_lock.py::test_an_interrupted_mutex_waiter_does_not_leak_it",
        "test_sync_semaphore.py::test_an_interrupted_waiter_does_not_leak_permits",
        "test_sync_semaphore.py::test_an_interrupted_limiter_waiter_does_not_leak_a_token",
        "test_sync_event_notify.py::test_an_interrupted_waiter_leaves_the_queue",
    ),
    "SY14": (
        "test_sync_lock.py::test_a_waiter_cancelled_after_the_handoff_passes_the_lock_on",
        "test_sync_semaphore.py::test_a_waiter_cancelled_after_its_grant_passes_the_permits_on",
        "test_sync_rwlock.py::test_a_waiter_cancelled_after_its_grant_passes_the_lock_on",
        "test_sync_event_notify.py::test_a_waiter_cancelled_after_notify_one_passes_the_wake_on",
    ),
    "SY15": (
        "test_sync_lock.py::test_acquire_does_nothing_until_awaited",
        "test_sync_semaphore.py::test_acquire_does_nothing_until_awaited",
        "test_sync_rwlock.py::test_read_and_write_do_nothing_until_entered",
        "test_sync_event_notify.py::test_notified_and_wait_do_nothing_until_awaited",
        "test_sync_condition_barrier.py::test_wait_does_nothing_until_awaited",
        "test_sync_condition_barrier.py::test_barrier_wait_does_nothing_until_awaited",
    ),
    "SY16": (
        "test_sync_lock.py::test_a_loop_of_uncontended_async_with_lets_other_tasks_run",
        "test_sync_lock.py::test_a_lock_is_free_after_the_run_closes_an_acquire_release_loop",
        "test_sync_lock.py::test_a_lock_is_free_after_the_run_closes_an_async_with_loop",
        "test_sync_lock.py::test_a_mutex_is_free_after_the_run_closes_an_async_with_loop",
        "test_sync_semaphore.py::test_a_cancelled_task_entering_takes_nothing",
        "test_sync_condition_barrier.py::test_a_waiter_freed_with_an_unobserved_relock_grant_passes_it_on_cleanly",
    ),
    "SY17": (
        "test_sync_lock.py::test_entering_a_free_lock_takes_it_when_awaited_without_suspending",
        "test_sync_lock.py::test_entering_takes_nothing_until_awaited",
        "test_sync_lock.py::test_an_exception_between_aenter_and_the_block_leaves_the_lock_free",
        "test_sync_lock.py::test_a_cancelled_task_entering_a_free_lock_gets_cancelled_and_takes_nothing",
        "test_sync_semaphore.py::test_entering_takes_a_free_permit_when_awaited_without_suspending",
        "test_sync_rwlock.py::test_entering_a_free_lock_takes_it_when_awaited_without_suspending",
    ),
    "SY18": (
        "test_sync_lock.py::test_acquire_outside_run_says_where_to_await",
        "test_sync_lock.py::test_entering_outside_a_runtime_says_where_to_await",
        "test_sync_semaphore.py::test_acquire_outside_run_says_where_to_await",
        "test_sync_event_notify.py::test_event_wait_outside_run_says_where_to_await",
        "test_channel.py::test_waiting_outside_a_porto_task_raises_no_runtime_error",
        "test_oneshot.py::test_recv_outside_a_runtime_that_must_wait_raises_no_runtime_error",
    ),
    "SY19": (
        "test_sync_lock.py::test_blocking_acquire_from_a_thread_and_refused_on_a_lane",
        "test_sync_semaphore.py::test_blocking_acquire_from_a_thread_and_refused_on_a_lane",
        "test_sync_rwlock.py::test_blocking_acquires_from_a_thread_and_refused_on_a_lane",
        "test_sync_event_notify.py::test_event_blocking_wait_from_a_thread",
        "test_sync_event_notify.py::test_event_blocking_wait_on_a_lane_raises",
        "test_sync_event_notify.py::test_notify_blocking_notified_from_a_thread_and_refused_on_a_lane",
        "test_sync_condition_barrier.py::test_threads_and_a_task_meet_at_a_barrier",
        "test_channel.py::test_blocking_send_from_threads_into_an_async_receiver",
        "test_channel.py::test_blocking_recv_from_a_thread_with_an_async_sender",
        "test_channel.py::test_blocking_calls_on_a_lane_raise_runtime_error_with_what_to_do",
        "test_channel.py::test_blocking_calls_report_the_end_and_a_closed_handle",
        "test_oneshot.py::test_blocking_recv_waits_for_a_send_from_a_thread",
        "test_oneshot.py::test_blocking_recv_on_a_lane_raises_runtime_error_with_what_to_do",
        "test_watch.py::test_blocking_changed_waits_for_a_send_from_the_main_thread",
        "test_watch.py::test_blocking_changed_on_a_lane_raises_runtime_error_with_what_to_do",
        "test_broadcast.py::test_blocking_recv_waits_for_a_send_from_another_thread",
        "test_broadcast.py::test_blocking_recv_on_a_lane_raises_runtime_error_with_what_to_do",
    ),
    "SY21": (
        "test_sync_lock.py::test_threads_serialize_on_a_lock",
        "test_sync_semaphore.py::test_threads_and_tasks_never_exceed_the_permits",
        "test_sync_event_notify.py::test_notify_is_shared_across_os_threads",
        "test_sync_condition_barrier.py::test_a_condition_hands_work_from_a_thread",
        "test_channel.py::test_blocking_send_from_threads_into_an_async_receiver",
        "test_watch.py::test_threads_borrowing_while_another_sends_never_see_a_freed_value",
        "test_broadcast.py::test_senders_on_several_threads_reach_every_receiver",
        "test_broadcast.py::test_receiver_threads_share_messages_without_leaking_references",
    ),
    "SY22": (
        "test_channel.py::test_channel_requires_a_capacity_and_says_how_to_pass_one",
        "test_channel.py::test_channel_capacity_must_be_an_int",
        "test_channel.py::test_channel_capacity_must_be_at_least_one",
        "test_channel.py::test_the_constructors_and_handles_are_subscriptable_at_runtime",
        "test_oneshot.py::test_the_constructor_is_subscriptable_and_makes_a_typed_pair",
        "test_watch.py::test_the_constructor_makes_a_typed_pair_with_a_readable_initial_value",
        "test_broadcast.py::test_the_constructor_makes_a_typed_pair",
        "test_broadcast.py::test_broadcast_capacity_must_be_at_least_one",
    ),
    "SY23": (
        "test_channel.py::test_capacity_is_the_bound_or_none",
        "test_channel.py::test_await_send_waits_while_the_channel_is_full",
        "test_channel.py::test_try_send_raises_would_block_when_full",
        "test_channel.py::test_an_unbounded_channel_never_blocks_a_sender",
        "test_channel.py::test_a_clone_keeps_the_channel_open",
        "test_channel.py::test_async_with_closes_the_sender",
        "test_channel.py::test_is_closed_once_every_receiver_is_gone",
        "test_channel.py::test_a_cancelled_send_waiter_passes_the_room_to_the_next_sender",
    ),
    "SY24": (
        "test_channel.py::test_recv_drains_the_buffer_then_raises_end_of_channel",
        "test_channel.py::test_try_recv_raises_would_block_when_empty_and_end_of_channel_when_ended",
        "test_channel.py::test_async_for_stops_at_the_end_of_the_channel",
        "test_channel.py::test_cloned_receivers_each_get_a_share_of_the_values",
        "test_channel.py::test_recv_many_returns_up_to_max_buffered_values",
        "test_channel.py::test_recv_many_waits_once_then_takes_everything_sent_meanwhile",
        "test_channel.py::test_recv_many_needs_max_of_at_least_one",
    ),
    "SY25": (
        "test_channel.py::test_a_waiting_recv_wakes_on_the_last_sender_closing",
        "test_channel.py::test_del_rx_makes_send_raise_broken_resource_error",
        "test_channel.py::test_dropping_the_last_receiver_wakes_a_waiting_sender",
        "test_channel.py::test_del_of_the_last_sender_ends_the_channel_after_the_buffer",
        "test_channel.py::test_values_nobody_can_receive_any_more_are_released",
    ),
    "SY26": (
        "test_channel.py::test_a_closed_sender_raises_closed_resource_error",
        "test_channel.py::test_a_closed_receiver_raises_closed_resource_error_and_the_last_breaks_sends",
        "test_channel.py::test_try_send_raises_would_block_when_full",
        "test_channel.py::test_try_recv_raises_would_block_when_empty_and_end_of_channel_when_ended",
    ),
    "SY27": (
        "test_channel.py::test_closing_a_receiver_wakes_a_task_waiting_on_it",
        "test_channel.py::test_closing_a_sender_wakes_a_task_waiting_to_send_on_it",
        "test_channel.py::test_closing_a_handle_wakes_a_blocking_call_on_another_thread",
        "test_channel.py::test_a_closed_receiver_passes_on_the_value_it_was_woken_for",
        "test_channel.py::test_a_closed_sender_passes_on_the_room_it_was_woken_for",
    ),
    "SY28": (
        "test_channel.py::test_a_cancelled_scope_raises_cancelled_at_a_channel_op_that_would_complete",
        "test_channel.py::test_a_cancelled_scope_raises_cancelled_at_an_unbounded_send",
        "test_channel.py::test_a_cancelled_channel_op_does_not_report_a_closed_channel_either",
        "test_channel.py::test_a_shielded_channel_op_in_a_cancelled_scope_still_completes",
        "test_channel.py::test_a_task_cancelled_between_two_channel_ops_loses_nothing",
        "test_channel.py::test_a_cancelled_recv_waiter_passes_the_value_to_the_next_receiver",
        "test_channel.py::test_a_recv_woken_before_the_interrupt_keeps_its_value",
        "test_channel.py::test_synchronous_completions_spend_the_budget_so_other_tasks_run",
    ),
    "SY29": (
        "test_channel.py::test_an_operation_never_awaited_neither_sends_nor_loses_a_value",
        "test_channel.py::test_an_operation_is_awaited_once",
        "test_oneshot.py::test_an_operation_never_awaited_takes_nothing_and_warns",
        "test_watch.py::test_an_operation_never_awaited_changes_nothing_and_warns",
        "test_broadcast.py::test_an_operation_never_awaited_takes_nothing_and_warns",
    ),
    "SY30": (
        "test_oneshot.py::test_a_value_sent_before_the_receive_is_received_without_waiting",
        "test_oneshot.py::test_recv_waits_for_the_send",
        "test_oneshot.py::test_the_value_is_received_once_then_the_channel_has_ended",
        "test_oneshot.py::test_a_dropped_sender_ends_the_receive_with_end_of_channel",
        "test_oneshot.py::test_send_to_a_dropped_receiver_raises_broken_resource_error",
        "test_oneshot.py::test_a_sender_is_used_up_by_its_send",
        "test_oneshot.py::test_async_for_yields_the_one_value_then_ends",
    ),
    "SY31": (
        "test_oneshot.py::test_two_receives_on_one_receiver_split_the_value_and_the_end",
        "test_oneshot.py::test_a_cancelled_receive_loses_no_value",
        "test_oneshot.py::test_a_receive_woken_before_the_interrupt_keeps_its_value",
    ),
    "SY32": (
        "test_watch.py::test_the_initial_value_is_seen_and_changed_waits_for_a_send",
        "test_watch.py::test_changed_returns_once_per_call_however_many_sends_came_before",
        "test_watch.py::test_borrow_does_not_mark_the_version_seen_but_borrow_and_update_does",
        "test_watch.py::test_async_for_yields_the_latest_value_per_change_and_ends_with_the_channel",
    ),
    "SY33": (
        "test_watch.py::test_every_clone_and_subscriber_has_its_own_seen_version",
        "test_watch.py::test_the_clearable_event_idiom_waits_until_the_value_is_true",
        "test_watch.py::test_changed_raises_end_of_channel_after_the_last_sender_closes",
        "test_watch.py::test_the_channel_ends_only_with_the_last_sender",
        "test_watch.py::test_send_without_a_receiver_is_broken_until_someone_subscribes",
    ),
    "SY34": (
        "test_broadcast.py::test_n_receivers_each_see_every_message_in_order",
        "test_broadcast.py::test_a_subscriber_sees_only_later_messages_and_a_clone_starts_where_its_original_is",
        "test_broadcast.py::test_recv_waits_and_every_waiting_receiver_wakes",
    ),
    "SY35": (
        "test_broadcast.py::test_a_slow_receiver_gets_lagged_with_the_exact_count_then_the_oldest_kept",
        "test_broadcast.py::test_lagged_is_raised_by_recv_and_resumes_at_the_oldest_kept",
        "test_broadcast.py::test_a_receiver_exactly_capacity_behind_is_not_lagged",
        "test_broadcast.py::test_lag_counts_add_up_to_everything_sent",
        "test_broadcast.py::test_async_for_raises_lagged_out_of_the_loop_and_a_new_loop_continues",
    ),
    "SY36": (
        "test_broadcast.py::test_closing_all_senders_ends_every_receiver_after_it_drains",
        "test_broadcast.py::test_send_without_a_receiver_is_broken_until_someone_subscribes",
        "test_broadcast.py::test_a_message_is_shared_not_copied_and_released_with_the_last_holder",
    ),
    "SY37": (
        "test_oneshot.py::test_async_with_closes_the_sender_on_the_way_out",
        "test_watch.py::test_a_closed_sender_raises_closed_resource_error_and_its_clones_are_closed",
        "test_watch.py::test_a_closed_receiver_raises_closed_resource_error_and_leaves_the_others",
        "test_broadcast.py::test_a_closed_sender_raises_closed_resource_error_and_its_clones_are_closed",
        "test_broadcast.py::test_try_recv_is_would_block_until_something_is_sent",
    ),
    "SY38": (
        "test_watch.py::test_a_cancelled_changed_loses_no_change",
        "test_watch.py::test_a_changed_woken_before_the_interrupt_completes",
        "test_broadcast.py::test_a_cancelled_recv_loses_nothing_and_does_not_disturb_the_other_receivers",
        "test_broadcast.py::test_a_recv_woken_before_the_interrupt_keeps_its_message",
    ),
}


def _functions(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def test_the_rule_index_names_tests_that_exist() -> None:
    missing: list[str] = []
    for tests in RULE_INDEX.values():
        for entry in tests:
            file, prefix = entry.split("::")
            path = TESTS / file
            if not path.is_file() or not any(f.startswith(prefix) for f in _functions(path)):
                missing.append(entry)
    assert not missing, f"RULE_INDEX names tests that are gone, update it: {missing}"


async def settle() -> None:
    """Lets every other ready task run until it waits."""
    for _ in range(10):
        await porto.checkpoint()


def completes_at_once(awaitable: Any) -> bool:
    """Resumes ``awaitable`` once by hand: whether it completed without suspending."""
    steps = awaitable.__await__()
    try:
        steps.send(None)
    except StopIteration:
        return True
    steps.close()
    return False


# --- §1 the primitives ----------------------------------------------------------------------


def test_SY1_lock_acquire_release_and_not_reentrant() -> None:
    async def main() -> None:
        lock = porto.Lock()
        assert not lock.locked()
        await lock.acquire()
        assert lock.locked()
        assert not lock.try_acquire()
        with porto.move_on_after(0.05) as again:  # the holder asking again waits forever
            await lock.acquire()
        assert again.timed_out
        lock.release()
        assert lock.try_acquire()
        lock.release()
        async with lock:
            assert lock.locked()
        assert not lock.locked()
        with pytest.raises(RuntimeError):
            lock.release()

    porto.run(main)


def test_SY2_mutex_guard_gives_the_value_and_dies_with_the_block() -> None:
    mutex = porto.Mutex[int](1)

    async def main() -> porto.MutexGuard[int]:
        async with mutex as guard:
            assert mutex.locked()
            guard.set(guard.value + 1)
        assert not mutex.locked()
        async with mutex as guard:
            assert guard.value == 2
        return guard

    guard = porto.run(main)
    with pytest.raises(RuntimeError, match="after its `async with` block"):
        _ = guard.value


def test_SY3_rwlock_readers_share_and_a_waiting_writer_goes_first() -> None:
    lock = porto.RwLock()
    order: list[str] = []

    async def writer() -> None:
        async with lock.write():
            order.append("writer")

    async def late_reader() -> None:
        async with lock.read():
            order.append("late reader")

    async def main() -> None:
        await lock.acquire_read()
        await lock.acquire_read()
        assert lock.readers == 2, "readers share"
        assert not lock.try_acquire_write()
        async with porto.scope() as s:
            s.spawn(writer)
            await settle()
            assert not lock.try_acquire_read(), "a reader queues behind the waiting writer"
            s.spawn(late_reader)
            await settle()
            assert order == []
            lock.release_read()
            lock.release_read()
        assert order == ["writer", "late reader"]
        assert lock.readers == 0
        assert not lock.write_locked()
        with pytest.raises(RuntimeError):
            lock.release_read()
        with pytest.raises(RuntimeError):
            lock.release_write()

    porto.run(main)


def test_SY4_semaphore_is_fair_and_bounded() -> None:
    order: list[str] = []

    async def main() -> None:
        sem = porto.Semaphore(3)
        assert sem.available == 3
        await sem.acquire(2)
        assert sem.available == 1
        assert not sem.try_acquire(2)
        assert sem.try_acquire()

        async def want(name: str, count: int) -> None:
            await sem.acquire(count)
            order.append(name)

        async with porto.scope() as s:
            s.spawn(want, "big", 2)
            await settle()
            s.spawn(want, "small", 1)
            await settle()
            assert order == [], "the small request waits behind the big one"
            sem.release(1)
            await settle()
            assert order == [], "one free permit is not enough for the big one at the front"
            sem.release(1)
            await settle()
            assert order == ["big"]
            sem.release(1)
            await settle()
            assert order == ["big", "small"]
        with pytest.raises(ValueError, match="the count must be from 1 to 3"):
            sem.release(10)  # more than were acquired
        with pytest.raises(ValueError, match="the count must be from 1 to 3"):
            await sem.acquire(4)  # could never be granted

    porto.run(main)
    with pytest.raises(ValueError, match="needs 1 to"):
        porto.Semaphore(0)


def test_SY5_capacity_limiter_tokens_and_borrowers() -> None:
    async def main() -> None:
        limiter = porto.CapacityLimiter(1)
        assert (limiter.total_tokens, limiter.available_tokens) == (1, 1)
        await limiter.acquire()
        assert limiter.borrowed_tokens == 1
        with pytest.raises(RuntimeError):
            await limiter.acquire()  # a borrower holds at most one token
        with pytest.raises(porto.WouldBlock):
            limiter.acquire_on_behalf_of_nowait("other")
        limiter.total_tokens = 2  # raising it makes a token free
        limiter.acquire_on_behalf_of_nowait("other")
        limiter.release_on_behalf_of("other")
        with pytest.raises(RuntimeError):
            limiter.release_on_behalf_of("other")  # not held any more
        limiter.release()
        limiter.total_tokens = math.inf
        assert limiter.total_tokens == math.inf

    porto.run(main)


def test_SY6_event_wakes_every_waiter_and_is_not_clearable() -> None:
    woke: list[int] = []

    async def waiter(n: int, event: porto.Event) -> None:
        await event.wait()
        woke.append(n)

    async def main() -> None:
        event = porto.Event()
        assert not event.is_set()
        async with porto.scope() as s:
            for n in range(3):
                s.spawn(waiter, n, event)
            await settle()
            assert woke == []
            event.set()
        assert sorted(woke) == [0, 1, 2]
        assert event.is_set()
        assert completes_at_once(event.wait())
        assert not hasattr(event, "clear")

    porto.run(main)


def test_SY7_notify_stores_one_permit_and_notify_all_wakes_only_the_waiting() -> None:
    log: list[str] = []

    async def waiter(name: str, notify: porto.Notify) -> None:
        await notify.notified()
        log.append(name)

    async def main() -> None:
        notify = porto.Notify()
        notify.notify_one()  # nobody waits: stored
        notify.notify_one()  # at most one permit is kept
        assert completes_at_once(notify.notified())
        assert not completes_at_once(notify.notified())
        async with porto.scope() as s:
            s.spawn(waiter, "a", notify)
            s.spawn(waiter, "b", notify)
            await settle()
            assert log == []
            notify.notify_one()
            await settle()
            assert log == ["a"], "the longest waiter first"
            notify.notify_all()
            await settle()
            assert log == ["a", "b"]
        notify.notify_all()  # nobody waits: nothing stored
        assert not completes_at_once(notify.notified())

    porto.run(main)


def test_SY8_condition_wait_gives_the_lock_up_and_takes_it_back_even_when_cancelled() -> None:
    condition = porto.Condition()
    events: list[str] = []

    async def waiter() -> None:
        async with condition:
            await condition.wait()
            events.append(f"notified, locked={condition.locked()}")

    async def cancelled_waiter() -> None:
        async with condition:
            try:
                await condition.wait()
            except porto.Cancelled:
                events.append(f"cancelled, locked={condition.locked()}")
                raise

    async def main() -> None:
        with pytest.raises(RuntimeError):
            await condition.wait()  # without the lock
        with pytest.raises(RuntimeError):
            condition.notify()
        async with porto.scope() as s:
            victim = s.spawn(cancelled_waiter)
            await settle()
            s.spawn(waiter)
            await settle()
            assert not condition.locked(), "wait released it"
            victim.cancel()
            await settle()
            async with condition:
                condition.notify()
        assert events == ["cancelled, locked=True", "notified, locked=True"]

        ready: list[int] = []

        async def filler() -> None:
            for n in range(3):
                async with condition:
                    ready.append(n)
                    condition.notify_all()
                await porto.checkpoint()

        async with porto.scope() as s, condition:
            s.spawn(filler)
            assert await condition.wait_for(lambda: len(ready) >= 3)
        assert len(ready) == 3

    porto.run(main)
    with pytest.raises(TypeError):
        porto.Condition(threading.Lock())  # type: ignore[arg-type]


def test_SY9_condition_notifications_are_not_stored_and_go_to_the_longest_waiter() -> None:
    log: list[str] = []

    async def waiter(name: str, condition: porto.Condition) -> None:
        async with condition:
            await condition.wait()
            log.append(name)

    async def main() -> None:
        condition = porto.Condition()
        async with condition:
            condition.notify()  # nobody waits: lost
        async with porto.scope() as s:
            first = s.spawn(waiter, "first", condition)
            await settle()
            s.spawn(waiter, "second", condition)
            s.spawn(waiter, "third", condition)
            await settle()
            assert log == [], "the earlier notify was not stored"
            first.cancel()  # a cancelled waiter is skipped
            await settle()
            async with condition:
                condition.notify()
            await settle()
            assert log == ["second"], "the longest remaining waiter"
            async with condition:
                condition.notify_all()
        assert log == ["second", "third"]

    porto.run(main)


def test_SY10_a_closed_condition_wait_leaves_the_holders_lock_alone() -> None:
    # The run ends with a waiter parked (lock released) and main holding the lock: the closed
    # wait cannot take the lock back, so the waiter's `async with` exit releases nothing.
    condition = porto.Condition()
    exits: list[str] = []

    async def waiter() -> None:
        try:
            async with condition:
                await condition.wait()
        except BaseException as error:
            exits.append(type(error).__name__)
            raise

    async def main() -> None:
        _core._spawn_joinable(waiter())
        await settle()
        await condition.acquire()

    _core._run(main())
    assert exits == ["GeneratorExit"], "the close went up as GeneratorExit"
    assert condition.locked(), "the closed wait released a lock another task held"
    condition.release()
    assert not condition.locked()


def test_SY11_barrier_has_one_leader_per_round_and_is_reusable() -> None:
    leaders: list[int] = []

    async def party(barrier: porto.Barrier) -> None:
        for round_number in range(3):
            if await barrier.wait():
                leaders.append(round_number)

    async def main() -> None:
        barrier = porto.Barrier(3)
        assert barrier.parties == 3
        async with porto.scope() as s:
            for _ in range(3):
                s.spawn(party, barrier)
        assert leaders == [0, 1, 2]
        assert barrier.n_waiting == 0

        async with porto.scope() as s:
            quitter = s.spawn(barrier.wait)
            await settle()
            assert barrier.n_waiting == 1
            quitter.cancel()
            await settle()
            assert barrier.n_waiting == 0, "the cancelled task left its round"

    porto.run(main)
    with pytest.raises(ValueError, match="at least one party"):
        porto.Barrier(0)


def test_SY12_waiters_are_served_in_arrival_order() -> None:
    order: list[int] = []

    async def waiter(n: int, lock: porto.Lock) -> None:
        async with lock:
            order.append(n)

    async def main() -> None:
        lock = porto.Lock()
        async with porto.scope() as s:
            await lock.acquire()
            for n in range(5):
                s.spawn(waiter, n, lock)
                await settle()
            lock.release()
        assert order == [0, 1, 2, 3, 4]

    porto.run(main)


def test_SY13_a_cancelled_waiter_is_removed_from_the_queue() -> None:
    order: list[str] = []

    async def waiter(name: str, lock: porto.Lock) -> None:
        async with lock:
            order.append(name)

    async def main() -> None:
        lock = porto.Lock()
        async with porto.scope() as s:
            await lock.acquire()
            s.spawn(waiter, "first", lock)
            await settle()
            middle = s.spawn(waiter, "middle", lock)
            await settle()
            s.spawn(waiter, "last", lock)
            await settle()
            middle.cancel()
            await settle()
            lock.release()
        assert order == ["first", "last"]
        assert not lock.locked()

    porto.run(main)


def test_SY14_a_waiter_cancelled_after_its_grant_passes_the_grant_on() -> None:
    lock = porto.Lock()

    async def waiter() -> None:
        await lock.acquire()

    async def main() -> None:
        await lock.acquire()
        _core._spawn_joinable(waiter())
        _core._spawn_joinable(waiter())
        await settle()
        # Hands the lock to the first waiter, which never runs again: the run ends and cancels
        # both. The first passes the grant to the second, which releases it.
        lock.release()
        assert lock.locked()

    _core._run(main())
    assert not lock.locked(), "the grant was lost"


def test_SY15_a_wait_never_awaited_takes_nothing_and_warns() -> None:
    async def main() -> None:
        lock, sem, event = porto.Lock(), porto.Semaphore(1), porto.Event()
        pending = [lock.acquire(), sem.acquire(), event.wait()]
        with pytest.warns(RuntimeWarning, match="was never awaited"):
            pending.clear()
        assert not lock.locked()
        assert sem.available == 1

    porto.run(main)


def test_SY16_a_task_cancelled_at_a_wait_never_holds_a_grant_it_did_not_see() -> None:
    async def main() -> None:
        lock = porto.Lock()
        sem = porto.Semaphore(1)
        async with porto.scope() as s:
            s.cancel()
            with pytest.raises(porto.Cancelled):
                await lock.acquire()
            with pytest.raises(porto.Cancelled):
                await sem.acquire()
        assert not lock.locked()
        assert sem.available == 1

    porto.run(main)


def test_SY17_async_with_takes_a_free_lock_without_suspending() -> None:
    async def main() -> None:
        lock = porto.Lock()
        entry = lock.__aenter__()
        assert not lock.locked(), "the call takes nothing"
        assert completes_at_once(entry)
        assert lock.locked()
        await lock.__aexit__(None, None, None)
        assert not lock.locked()
        async with porto.scope() as s:
            s.cancel()
            with pytest.raises(porto.Cancelled):
                async with lock:
                    raise AssertionError("entered a cancelled scope")
        assert not lock.locked()
        with pytest.warns(RuntimeWarning, match="never awaited"):
            lock.__aenter__()  # dropped at once: an exception before the await leaves it free
        assert not lock.locked()

    porto.run(main)


def test_SY18_waiting_outside_a_runtime_raises_no_runtime_error() -> None:
    lock = porto.Lock()
    assert lock.try_acquire()
    with pytest.raises(porto.NoRuntimeError):
        lock.acquire().__await__().send(None)
    with pytest.raises(porto.NoRuntimeError):
        porto.Event().wait().__await__().send(None)
    _tx, rx = porto.channel[int](1)
    with pytest.raises(porto.NoRuntimeError):
        rx.recv().__await__().send(None)


def test_SY19_blocking_calls_work_off_the_lanes_and_raise_on_a_lane() -> None:
    event = porto.Event()
    tx, rx = porto.channel[int](1)

    async def main() -> None:
        calls: list[Any] = [
            porto.Lock().blocking_acquire,
            porto.Semaphore(1).blocking_acquire,
            porto.RwLock().blocking_acquire_read,
            porto.RwLock().blocking_acquire_write,
            event.blocking_wait,
            porto.Notify().blocking_notified,
            porto.Barrier(2).blocking_wait,
            lambda: tx.blocking_send(1),
            rx.blocking_recv,
        ]
        for call in calls:
            with pytest.raises(RuntimeError, match="blocking_"):
                call()

    porto.run(main)
    woken = threading.Event()

    def thread() -> None:
        event.blocking_wait()
        woken.set()

    worker = threading.Thread(target=thread)
    worker.start()
    event.set()
    worker.join(LATE)
    assert woken.is_set()
    tx.blocking_send(7)
    assert rx.blocking_recv() == 7


@pytest.mark.skipif(
    signal.getsignal(signal.SIGINT) is not signal.default_int_handler,
    reason="needs the default SIGINT handler (the main thread of a plain pytest run)",
)
def test_SY20_a_blocking_wait_is_not_interrupted_by_a_signal() -> None:
    event = porto.Event()
    pause = threading.Event()

    def interrupt_then_set() -> None:
        pause.wait(0.3)  # the main thread is in blocking_wait by now
        _thread.interrupt_main()
        pause.wait(0.2)  # the interrupt has landed; the wait must go on
        event.set()

    worker = threading.Thread(target=interrupt_then_set)
    worker.start()
    with pytest.raises(KeyboardInterrupt):
        event.blocking_wait()
    worker.join(LATE)
    assert event.is_set(), "the wait ran to its end before the KeyboardInterrupt was raised"


def test_SY21_threads_share_a_lock_and_a_semaphore() -> None:
    lock = porto.Lock()
    semaphore = porto.Semaphore(2)
    counter = [0]
    inside = [0]
    peak = [0]
    guard = threading.Lock()

    def work() -> None:
        for _ in range(100):
            lock.blocking_acquire()
            counter[0] += 1
            lock.release()
            semaphore.blocking_acquire()
            with guard:
                inside[0] += 1
                peak[0] = max(peak[0], inside[0])
            with guard:
                inside[0] -= 1
            semaphore.release()

    threads = [threading.Thread(target=work) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(LATE * 3)
    assert counter[0] == 400
    assert peak[0] <= 2
    assert semaphore.available == 2


# --- §2 the channels ------------------------------------------------------------------------


def test_SY22_the_constructors_check_the_capacity_and_every_class_subscripts() -> None:
    with pytest.raises(TypeError, match=r"unbounded_channel\[T\]\(\)"):
        porto.channel()  # pyright: ignore[reportCallIssue]
    with pytest.raises(TypeError):
        porto.channel("4")  # pyright: ignore[reportArgumentType]
    for capacity in (0, -1):
        with pytest.raises(ValueError, match="unbounded_channel"):
            porto.channel(capacity)
    classes: list[Any] = [
        porto.Mutex,
        porto.MutexGuard,
        porto.Sender,
        porto.Receiver,
        porto.OneshotSender,
        porto.OneshotReceiver,
        porto.WatchSender,
        porto.WatchReceiver,
        porto.BroadcastSender,
        porto.BroadcastReceiver,
    ]
    for cls in classes:
        assert cls[int] is not None
    assert porto.channel[int](1) is not None
    assert porto.unbounded_channel[int]() is not None
    assert porto.oneshot[int]() is not None
    assert porto.watch[int](0) is not None
    assert porto.broadcast[int](1) is not None


def test_SY23_sender_api() -> None:
    async def main() -> None:
        tx, rx = porto.channel[int](1)
        assert tx.capacity == 1
        assert not tx.is_closed()
        tx.try_send(1)
        with pytest.raises(porto.WouldBlock):
            tx.try_send(2)
        order: list[str] = []

        async def sender(name: str, value: int) -> None:
            await tx.send(value)
            order.append(name)

        async with porto.scope() as s:
            s.spawn(sender, "a", 2)
            await settle()
            s.spawn(sender, "b", 3)
            await settle()
            assert order == [], "a full channel makes the sender wait"
            assert rx.try_recv() == 1
            await settle()
            assert order == ["a"], "room goes to the senders in arrival order"
            assert rx.try_recv() == 2
            await settle()
            assert order == ["a", "b"]
        other = tx.clone()
        other.close()
        assert not tx.is_closed()
        async with tx:
            pass
        assert tx.is_closed()

    porto.run(main)


def test_SY24_receiver_api() -> None:
    async def main() -> None:
        tx, rx = porto.channel[int](8)
        for n in range(6):
            tx.try_send(n)
        assert await rx.recv() == 0
        assert rx.try_recv() == 1
        assert await rx.recv_many(3) == [2, 3, 4]
        clone = rx.clone()
        assert clone.try_recv() == 5
        with pytest.raises(porto.WouldBlock):
            rx.try_recv()
        tx.try_send(10)
        tx.try_send(11)
        assert await rx.recv_many(10) == [10, 11], "everything buffered, up to max"
        tx.try_send(12)
        tx.close()
        assert [v async for v in rx] == [12]
        rx.close()
        with pytest.raises(porto.ClosedResourceError):
            rx.try_recv()

    porto.run(main)


def test_SY25_a_channel_closes_with_its_last_sender_or_its_last_receiver() -> None:
    async def main() -> None:
        tx, rx = porto.channel[int](4)
        tx.try_send(1)
        other = tx.clone()
        tx.close()
        other.try_send(2)  # a clone keeps it open
        del other  # dropping closes: the last sender is gone
        assert await rx.recv() == 1
        assert await rx.recv() == 2
        with pytest.raises(porto.EndOfChannel):
            await rx.recv()

        tx, rx = porto.channel[int](4)
        del rx
        with pytest.raises(porto.BrokenResourceError):
            tx.try_send(1)
        with pytest.raises(porto.BrokenResourceError):
            await tx.send(1)

    porto.run(main)


def test_SY26_a_closed_handle_raises_closed_resource_error() -> None:
    async def main() -> None:
        tx, rx = porto.channel[int](1)
        tx.close()
        with pytest.raises(porto.ClosedResourceError):
            tx.try_send(1)
        with pytest.raises(porto.ClosedResourceError):
            await tx.send(1)
        with pytest.raises(porto.EndOfChannel):
            await rx.recv()  # the other side ended: not "closed by the program"
        rx.close()
        with pytest.raises(porto.ClosedResourceError):
            await rx.recv()
        tx, rx = porto.channel[int](1)
        with pytest.raises(porto.WouldBlock):
            rx.try_recv()
        tx.try_send(1)
        with pytest.raises(porto.WouldBlock):
            tx.try_send(2)

    porto.run(main)


def test_SY27_closing_a_handle_wakes_its_waiters_and_leaves_the_others() -> None:
    async def main() -> None:
        tx, rx = porto.channel[int](1)
        other = rx.clone()
        outcomes: dict[str, str] = {}

        async def receive(name: str, handle: porto.Receiver[int]) -> None:
            try:
                await handle.recv()
                outcomes[name] = "value"
            except porto.ClosedResourceError:
                outcomes[name] = "closed"

        async with porto.scope() as s:
            s.spawn(receive, "closed", rx)
            s.spawn(receive, "other", other)
            await settle()
            rx.close()
            await settle()
            assert outcomes == {"closed": "closed"}, "the waiter on the other handle waits on"
            tx.try_send(5)
        assert outcomes == {"closed": "closed", "other": "value"}

    porto.run(main)


def test_SY28_a_channel_op_is_a_checkpoint_and_a_cancelled_waiter_loses_nothing() -> None:
    async def main() -> None:
        tx, rx = porto.channel[int](2)
        tx.try_send(1)
        async with porto.scope() as s:
            s.cancel()
            with pytest.raises(porto.Cancelled):
                await tx.send(9)
            with pytest.raises(porto.Cancelled):
                await rx.recv()
            async with porto.shield():
                await tx.send(2)  # a shield suspends the checkpoint
        assert [rx.try_recv(), rx.try_recv()] == [1, 2]

        tx.close()
        async with porto.scope() as s:
            s.cancel()
            with pytest.raises(porto.Cancelled):  # not ClosedResourceError
                await tx.send(3)

        tx, rx = porto.channel[int](1)
        got: list[int] = []

        async def receive() -> None:
            got.append(await rx.recv())

        async with porto.scope() as s:
            victim = s.spawn(receive)
            await settle()
            s.spawn(receive)
            await settle()
            victim.cancel()
            await settle()
            tx.try_send(7)
        assert got == [7], "the value went to the receiver that was not cancelled"

    porto.run(main)


def test_SY29_a_channel_op_never_awaited_sends_and_takes_nothing() -> None:
    async def main() -> None:
        tx, rx = porto.channel[int](1)
        tx.try_send(1)
        pending = [tx.send(2), rx.recv(), rx.recv_many(1)]
        with pytest.warns(RuntimeWarning, match="never awaited"):
            pending.clear()
        assert rx.try_recv() == 1
        with pytest.raises(porto.WouldBlock):
            rx.try_recv()

    porto.run(main)


def test_SY30_oneshot_carries_one_value() -> None:
    async def main() -> None:
        tx, rx = porto.oneshot[str]()
        tx.send("v")
        with pytest.raises(porto.ClosedResourceError):
            tx.send("again")
        assert await rx.recv() == "v"
        with pytest.raises(porto.EndOfChannel):
            await rx.recv()
        assert not hasattr(rx, "clone")

        tx, rx = porto.oneshot[str]()
        tx.close()
        with pytest.raises(porto.EndOfChannel):
            await rx.recv()
        tx, rx = porto.oneshot[str]()
        rx.close()
        with pytest.raises(porto.BrokenResourceError):
            tx.send("lost")
        tx, rx = porto.oneshot[str]()
        tx.send("one")
        assert [v async for v in rx] == ["one"]

    porto.run(main)


def test_SY31_oneshot_with_several_receivers_gives_the_value_to_one() -> None:
    outcomes: list[str] = []

    async def receive(rx: porto.OneshotReceiver[str]) -> None:
        try:
            outcomes.append(await rx.recv())
        except porto.EndOfChannel:
            outcomes.append("end")

    async def main() -> None:
        tx, rx = porto.oneshot[str]()
        async with porto.scope() as s:
            victim = s.spawn(receive, rx)
            await settle()
            s.spawn(receive, rx)
            s.spawn(receive, rx)
            await settle()
            victim.cancel()
            await settle()
            tx.send("v")
        assert sorted(outcomes) == ["end", "v"], "a cancelled receive lost nothing"

    porto.run(main)


def test_SY32_watch_versions_and_what_changed_marks_seen() -> None:
    async def main() -> None:
        tx, rx = porto.watch[int](0)
        assert rx.borrow() == 0
        assert not rx.has_changed()
        with pytest.raises(porto.WouldBlock):
            rx.try_changed()
        tx.send(1)
        tx.send(2)
        tx.send(3)
        assert rx.has_changed()
        assert rx.borrow() == 3
        assert rx.has_changed(), "borrow marks nothing"
        await rx.changed()  # three sends are one completion
        assert not rx.has_changed()
        tx.send(4)
        assert rx.borrow_and_update() == 4
        assert not rx.has_changed()
        async with porto.scope() as s:
            task = s.spawn(rx.changed)
            await settle()
            assert not task.done()
            tx.send(5)
        assert rx.borrow() == 5
        tx.send(6)
        tx.close()
        assert [v async for v in rx] == [6]  # the latest value after the change, once

    porto.run(main)


def test_SY33_watch_clone_subscribe_end_and_the_clearable_event() -> None:
    async def main() -> None:
        tx, rx = porto.watch[int](0)
        tx.send(1)
        assert rx.clone().has_changed(), "a clone has seen what the original had"
        await rx.changed()
        assert not rx.clone().has_changed()
        assert not tx.subscribe().has_changed()
        tx.send(2)
        tx.close()
        await rx.changed()  # the change it had not seen
        with pytest.raises(porto.EndOfChannel):
            await rx.changed()
        assert rx.borrow() == 2, "borrow keeps the last value"

        tx, rx = porto.watch[int](0)
        del rx
        with pytest.raises(porto.BrokenResourceError):
            tx.send(1)
        subscriber = tx.subscribe()
        assert subscriber.borrow() == 0, "the failed send stored nothing"
        tx.send(1)  # a receiver exists again
        assert subscriber.borrow() == 1

        flag_tx, flag_rx = porto.watch[bool](False)
        woke: list[str] = []

        async def wait_set() -> None:
            while not flag_rx.borrow():
                await flag_rx.changed()
            woke.append("set")

        async with porto.scope() as s:
            s.spawn(wait_set)
            await settle()
            assert woke == []
            flag_tx.send(True)
        assert woke == ["set"]
        flag_tx.send(False)
        assert not flag_rx.borrow()

    porto.run(main)


def test_SY34_broadcast_gives_every_receiver_every_message() -> None:
    async def main() -> None:
        tx, rx = porto.broadcast[int](4)
        other = rx.clone()
        tx.send(1)
        late = tx.subscribe()
        tx.send(2)
        assert [await rx.recv(), await rx.recv()] == [1, 2]
        assert [await other.recv(), await other.recv()] == [1, 2]
        assert await late.recv() == 2, "a subscriber starts at the next message"
        mid = rx.clone()
        tx.send(3)
        assert await mid.recv() == 3, "a clone starts where its original is"
        for n in range(10):  # never waits: it overwrites the oldest
            tx.send(n)

    porto.run(main)


def test_SY35_broadcast_lag_counts_what_was_overwritten() -> None:
    async def main() -> None:
        tx, rx = porto.broadcast[int](2)
        for n in range(5):
            tx.send(n)
        with pytest.raises(porto.Lagged) as lag:
            await rx.recv()
        assert lag.value.skipped == 3
        received = [await rx.recv(), await rx.recv()]
        assert received == [3, 4], "the oldest kept comes next"
        assert len(received) + lag.value.skipped == 5

    porto.run(main)


def test_SY36_broadcast_drains_then_ends_and_shares_the_object() -> None:
    async def main() -> None:
        tx, rx = porto.broadcast[list[int]](4)
        other = rx.clone()
        message = [1]
        tx.send(message)
        tx.close()
        assert await rx.recv() is message
        assert await other.recv() is message
        with pytest.raises(porto.EndOfChannel):
            await rx.recv()

        tx, rx = porto.broadcast[int](1)
        del rx
        with pytest.raises(porto.BrokenResourceError):
            tx.send(1)
        assert tx.subscribe() is not None

    porto.run(main)


def test_SY37_the_other_constructors_send_with_a_plain_call() -> None:
    async def main() -> None:
        makers: list[Any] = [porto.oneshot[int], lambda: porto.watch[int](0)]
        makers.append(lambda: porto.broadcast[int](1))
        for make in makers:
            tx, rx = make()
            assert tx.send(1) is None  # a plain call, not an awaitable
            assert not hasattr(tx, "try_send")
            assert hasattr(tx, "capacity") == isinstance(tx, porto.BroadcastSender)
            assert not hasattr(rx, "recv_many")

            tx, rx = make()
            rx.close()
            with pytest.raises(porto.BrokenResourceError):
                tx.send(1)  # every receiver is gone
            with pytest.raises(porto.ClosedResourceError):
                await rx.recv() if hasattr(rx, "recv") else await rx.changed()

            tx, rx = make()
            async with tx:
                pass
            with pytest.raises(porto.ClosedResourceError):
                tx.send(1)
            with pytest.raises(porto.EndOfChannel):
                await rx.recv() if hasattr(rx, "recv") else await rx.changed()

    porto.run(main)


def test_SY38_a_cancelled_watch_or_broadcast_waiter_loses_nothing() -> None:
    async def main() -> None:
        wtx, wrx = porto.watch[int](0)
        btx, brx = porto.broadcast[int](4)
        other = brx.clone()
        got: list[Any] = []

        async def watch_changed() -> None:
            await wrx.changed()
            got.append("changed")

        async def broadcast_recv() -> None:
            got.append(await brx.recv())

        async with porto.scope() as s:
            w = s.spawn(watch_changed)
            b = s.spawn(broadcast_recv)
            await settle()
            w.cancel()
            b.cancel()
            await settle()
            wtx.send(1)
            btx.send(2)
        assert got == []
        assert wrx.has_changed(), "the version stayed unseen"
        assert await brx.recv() == 2, "the position did not move"
        assert await other.recv() == 2, "and nobody else was disturbed"

    porto.run(main)


# --- §3 select ------------------------------------------------------------------------------


def test_SY39_select_is_not_provided() -> None:
    assert not hasattr(porto, "select")
    assert not hasattr(porto.lowlevel, "select")
