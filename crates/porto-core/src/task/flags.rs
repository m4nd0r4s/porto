//! Per-task flags: fixed at spawn, read-only afterwards (`11-task-model-and-trap-protocol.md`
//! §1).

use std::fmt;
use std::ops::BitOr;

/// The `flags` field of a task [`Header`](super::Header). Set once when the task is created and
/// never mutated, so any thread holding a reference may read it without synchronisation.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TaskFlags(u16);

impl TaskFlags {
    /// No flags: a Rust task that may run on any lane.
    pub const NONE: Self = Self(0);
    /// The task stays on the lane that spawned it (`spawn(pin=True)`, lane-local resources).
    pub const PIN: Self = Self(1 << 0);
    /// The task drives a Python coroutine (a `PyTask`), so polling it needs the interpreter.
    pub const PYTHON: Self = Self(1 << 1);
    /// Nobody joins the task: dropping it detached is expected, not a leaked result.
    pub const DETACHED_DROP: Self = Self(1 << 2);

    /// The raw bits.
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Whether every flag in `other` is set.
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }
}

impl BitOr for TaskFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl fmt::Debug for TaskFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = [
            (Self::PIN, "PIN"),
            (Self::PYTHON, "PYTHON"),
            (Self::DETACHED_DROP, "DETACHED_DROP"),
        ];
        let mut set = f.debug_set();
        for (flag, name) in names {
            if self.contains(flag) {
                set.entry(&format_args!("{name}"));
            }
        }
        set.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_combine_and_test_independently() {
        let flags = TaskFlags::PIN | TaskFlags::PYTHON;
        assert!(flags.contains(TaskFlags::PIN));
        assert!(flags.contains(TaskFlags::PYTHON));
        assert!(!flags.contains(TaskFlags::DETACHED_DROP));
        assert!(flags.contains(TaskFlags::NONE));
        assert_eq!(format!("{flags:?}"), "{PIN, PYTHON}");
    }
}
