import forwarder


def test_statsd_lines_from_samples_data_no_records():
    lines = forwarder.statsd_gauges_from_samples_data(
        {},
        {
            "entities": {},
        },
    )
    assert list(lines) == []


def test_statsd_lines_from_samples_data_statsd():
    game_data = {
        "virtual_signal_names": ["signal-A"],
        "item_names": ["coal"],
        "fluid_names": ["water"],
    }

    test_samples = {
        "entities": [
            {
                "settings": {
                    "name": "my_metric",
                    "tags": "base=alpha,planet=nauvis,ores",
                    "absent_signals": "ignore",
                },
                "red_signals": [
                    {
                        "signal": {
                            "type": "item",
                            "name": "coal",
                        },
                        "count": 2,
                    }
                ],
            }
        ],
    }

    [test_gauge] = forwarder.statsd_gauges_from_samples_data(game_data, test_samples)
    assert test_gauge == forwarder.Gauge(
        "my_metric",
        2,
        [
            forwarder.Tag("base", "alpha"),
            forwarder.Tag("planet", "nauvis"),
            forwarder.Tag("ores"),
            forwarder.Tag("signal_type", "item"),
            forwarder.Tag("signal_name", "coal"),
        ],
    )


def test_statsd_lines_from_samples_data_treat_as_0():
    game_data = {
        "virtual_signal_names": ["signal-A"],
        "item_names": ["coal"],
        "fluid_names": ["water"],
    }

    test_samples = {
        "entities": [
            {
                "settings": {
                    "name": "my_metric",
                    "tags": "base=alpha,planet=nauvis,ores",
                    "absent_signals": "treat-as-0",
                },
                "red_signals": [
                    {
                        "signal": {
                            "type": "item",
                            "name": "coal",
                        },
                        "count": 2,
                    }
                ],
            }
        ],
    }

    gauges = forwarder.statsd_gauges_from_samples_data(game_data, test_samples)
    assert list(gauges) == [
        forwarder.Gauge(
            "my_metric",
            2,
            [
                forwarder.Tag("base", "alpha"),
                forwarder.Tag("planet", "nauvis"),
                forwarder.Tag("ores"),
                forwarder.Tag("signal_type", "item"),
                forwarder.Tag("signal_name", "coal"),
            ],
        ),
        forwarder.Gauge(
            "my_metric",
            0,
            [
                forwarder.Tag("base", "alpha"),
                forwarder.Tag("planet", "nauvis"),
                forwarder.Tag("ores"),
                forwarder.Tag("signal_type", "virtual"),
                forwarder.Tag("signal_name", "signal-A"),
            ],
        ),
        forwarder.Gauge(
            "my_metric",
            0,
            [
                forwarder.Tag("base", "alpha"),
                forwarder.Tag("planet", "nauvis"),
                forwarder.Tag("ores"),
                forwarder.Tag("signal_type", "fluid"),
                forwarder.Tag("signal_name", "water"),
            ],
        ),
    ]


def test_statsd_packets_from_lines():
    packets = forwarder.statsd_packets_from_lines(["foo", "bar", "baz"], 7)
    assert list(packets) == [b"foo\nbar", b"baz"]
