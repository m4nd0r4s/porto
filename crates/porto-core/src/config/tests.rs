use std::time::Duration;

use super::*;

#[test]
fn defaults_match_the_python_reference() {
    let config = RuntimeConfig::builder().build().unwrap();
    assert_eq!(config, RuntimeConfig::default());
    assert_eq!(config.lanes(), Lanes::Fixed(1));
    assert_eq!(config.io_backend(), IoBackend::Auto);
    assert!(!config.io_backend_strict());
    assert_eq!(config.steal(), Steal::Adaptive);
    assert_eq!(config.python_steal(), PythonSteal::Adaptive);
    assert_eq!(config.python_steal_threshold(), 8);
    assert_eq!(config.coop_budget(), 128);
    assert_eq!(config.blocking_threads(), 512);
    assert_eq!(config.blocking_keepalive(), Duration::from_secs(10));
    assert_eq!(config.timer_resolution(), Duration::from_millis(1));
    assert!(config.asyncio_compat());
    assert_eq!(config.anyio_backend(), AnyioBackend::Native);
    assert!(config.handle_signals());
    assert!(!config.debug());
    assert_eq!(
        config.slow_step_threshold(),
        Some(Duration::from_millis(100))
    );
    assert_eq!(config.console(), None);
    assert!(config.metrics());
    assert_eq!(config.deterministic(), None);
    assert_eq!(config.graceful_timeout(), Duration::from_secs(10));
    assert_eq!(config.cancel_timeout(), Duration::from_secs(5));
}

#[test]
fn setters_store_their_values() {
    let deterministic = DeterministicConfig {
        seed: 7,
        ..DeterministicConfig::default()
    };
    let config = RuntimeConfig::builder()
        .lanes(Lanes::Auto)
        .io_backend(IoBackend::IoUring)
        .io_backend_strict(true)
        .steal(Steal::Eager)
        .python_steal(PythonSteal::Off)
        .python_steal_threshold(3)
        .coop_budget(64)
        .blocking_threads(4)
        .blocking_keepalive(Duration::from_secs(1))
        .timer_resolution(Duration::from_micros(500))
        .asyncio_compat(false)
        .anyio_backend(AnyioBackend::Facade)
        .handle_signals(false)
        .debug(true)
        .slow_step_threshold(None)
        .console(Some("127.0.0.1:6669"))
        .metrics(false)
        .deterministic(Some(deterministic.clone()))
        .graceful_timeout(Duration::from_secs(30))
        .cancel_timeout(Duration::ZERO)
        .build()
        .unwrap();
    assert_eq!(config.lanes(), Lanes::Auto);
    assert_eq!(config.io_backend(), IoBackend::IoUring);
    assert!(config.io_backend_strict());
    assert_eq!(config.steal(), Steal::Eager);
    assert_eq!(config.python_steal(), PythonSteal::Off);
    assert_eq!(config.python_steal_threshold(), 3);
    assert_eq!(config.coop_budget(), 64);
    assert_eq!(config.blocking_threads(), 4);
    assert_eq!(config.blocking_keepalive(), Duration::from_secs(1));
    assert_eq!(config.timer_resolution(), Duration::from_micros(500));
    assert!(!config.asyncio_compat());
    assert_eq!(config.anyio_backend(), AnyioBackend::Facade);
    assert!(!config.handle_signals());
    assert!(config.debug());
    assert_eq!(config.slow_step_threshold(), None);
    assert_eq!(config.console(), Some("127.0.0.1:6669"));
    assert!(!config.metrics());
    assert_eq!(config.deterministic(), Some(&deterministic));
    assert_eq!(config.graceful_timeout(), Duration::from_secs(30));
    assert_eq!(config.cancel_timeout(), Duration::ZERO);
}

#[test]
fn invalid_values_name_the_field_and_the_fix() {
    let cases: Vec<(&str, ConfigBuilder, &str)> = vec![
        (
            "lanes",
            RuntimeConfig::builder().lanes(Lanes::Fixed(0)),
            "Lanes::Auto",
        ),
        (
            "python_steal_threshold",
            RuntimeConfig::builder().python_steal_threshold(0),
            "use 1 or more",
        ),
        (
            "coop_budget",
            RuntimeConfig::builder().coop_budget(0),
            "use 1 or more",
        ),
        (
            "blocking_threads",
            RuntimeConfig::builder().blocking_threads(0),
            "use 1 or more",
        ),
        (
            "timer_resolution",
            RuntimeConfig::builder().timer_resolution(Duration::from_micros(99)),
            "100µs..=10ms",
        ),
        (
            "timer_resolution",
            RuntimeConfig::builder().timer_resolution(Duration::from_millis(11)),
            "100µs..=10ms",
        ),
        (
            "slow_step_threshold",
            RuntimeConfig::builder().slow_step_threshold(Some(Duration::ZERO)),
            "or None",
        ),
        (
            "console",
            RuntimeConfig::builder().console(Some("6669")),
            "127.0.0.1:6669",
        ),
        (
            "console",
            RuntimeConfig::builder().console(Some(":6669")),
            "host:port",
        ),
        (
            "console",
            RuntimeConfig::builder().console(Some("host:http")),
            "host:port",
        ),
        (
            "deterministic",
            RuntimeConfig::builder().deterministic(Some(DeterministicConfig {
                max_steps: Some(0),
                ..DeterministicConfig::default()
            })),
            "positive limit",
        ),
    ];
    for (field, builder, hint) in cases {
        let error = builder.build().unwrap_err();
        assert_eq!(error.field(), field);
        let message = error.to_string();
        assert!(
            message.starts_with(&format!("porto: invalid {field}: ")),
            "{message}"
        );
        assert!(message.contains(hint), "{message:?} lacks {hint:?}");
    }
}

#[test]
fn timer_resolution_bounds_are_inclusive() {
    for resolution in [MIN_TIMER_RESOLUTION, MAX_TIMER_RESOLUTION] {
        let config = RuntimeConfig::builder()
            .timer_resolution(resolution)
            .build()
            .unwrap();
        assert_eq!(config.timer_resolution(), resolution);
    }
}

#[test]
fn auto_lanes_follow_the_build_and_the_core_count() {
    // (gil_build, cores, expected)
    let cases = [
        (true, 1, 1),
        (true, 2, 2),
        (true, 16, 2),
        (false, 1, 1),
        (false, 8, 8),
        (false, 64, 64),
        (false, 65, 64),
        (false, 256, 64),
        (true, 0, 1),
        (false, 0, 1),
    ];
    for (gil_build, cores, expected) in cases {
        assert_eq!(
            Lanes::Auto.resolve_with_cores(gil_build, cores),
            expected,
            "gil_build={gil_build}, cores={cores}"
        );
    }
}

#[test]
fn fixed_lanes_ignore_the_build_and_the_core_count() {
    assert_eq!(Lanes::Fixed(4).resolve_with_cores(true, 1), 4);
    assert_eq!(Lanes::Fixed(100).resolve_with_cores(false, 8), 100);
}

#[test]
fn auto_lanes_on_this_machine_are_in_range() {
    assert!(physical_cores() >= 1);
    assert!((1..=MAX_AUTO_LANES).contains(&Lanes::Auto.resolve(false)));
    assert!((1..=2).contains(&Lanes::Auto.resolve(true)));
}

#[test]
fn enums_round_trip_through_their_lowercase_names() {
    fn round_trip<T>(names: &[&str])
    where
        T: std::str::FromStr<Err = UnknownName> + std::fmt::Display,
    {
        for name in names {
            assert_eq!(name.parse::<T>().unwrap().to_string(), *name);
        }
    }
    round_trip::<IoBackend>(IoBackend::NAMES);
    round_trip::<Steal>(Steal::NAMES);
    round_trip::<PythonSteal>(PythonSteal::NAMES);
    round_trip::<AnyioBackend>(AnyioBackend::NAMES);
    round_trip::<DeterministicClock>(DeterministicClock::NAMES);
    assert_eq!(
        IoBackend::NAMES,
        ["auto", "epoll", "kqueue", "iocp", "io_uring"]
    );
    assert_eq!(Lanes::Auto.to_string(), "auto");
    assert_eq!(Lanes::Fixed(3).to_string(), "3");
}

#[test]
fn unknown_enum_names_list_the_valid_ones() {
    let error = "Eager".parse::<Steal>().unwrap_err();
    assert_eq!(
        error.to_string(),
        "porto: \"Eager\" is not a valid steal; use one of: adaptive, off, eager"
    );
}

#[test]
fn io_backend_config_carries_the_backend_and_strictness() {
    let config = RuntimeConfig::builder()
        .io_backend(IoBackend::Kqueue)
        .io_backend_strict(true)
        .build()
        .unwrap();
    let io = config.io_backend_config();
    assert_eq!(io.backend, porto_io::IoBackend::Kqueue);
    assert!(io.strict);
    assert_eq!(
        RuntimeConfig::default().io_backend_config(),
        porto_io::IoBackendConfig::default()
    );
}
