from forwarder import metrics_from_samples_data, statsd_packets_from_lines, FLAVOR_LINE_FORMATTERS
import pytest


@pytest.mark.parametrize('test_flavor,expected_line', [
    ('vanilla', 'my_metric:2|g'),
    ('dogstatsd', 'my_metric:2|g|#base:alpha,planet:nauvis,ores,signal_type:item,signal_name:coal')
])
def test_vanilla_line_formatter(test_flavor, expected_line):
    test_metrics = {'name': 'my_metric', 'n': 2, 'tags': ['base:alpha', 'planet:nauvis', 'ores', 'signal_type:item', 'signal_name:coal']}

    line_formatter = FLAVOR_LINE_FORMATTERS[test_flavor]
    assert line_formatter(test_metrics) == expected_line


def test_metrics_from_samples_data_no_data():
    lines = metrics_from_samples_data({}, {
        'entities': {},
    })
    assert lines == []


@pytest.mark.parametrize('test_absent_signals,expected_metrics', [
    ('ignore', [
        {'name': 'my_metric', 'n': 2, 'tags': ['base:alpha', 'planet:nauvis', 'ores', 'signal_type:item', 'signal_name:coal']}
    ]),
    ('treat-as-0', [
        {'name': 'my_metric', 'n': 2, 'tags': ['base:alpha', 'planet:nauvis', 'ores', 'signal_type:item', 'signal_name:coal']},
        {'name': 'my_metric', 'n': 0, 'tags': ['base:alpha', 'planet:nauvis', 'ores', 'signal_type:virtual', 'signal_name:signal-A']},
        {'name': 'my_metric', 'n': 0, 'tags': ['base:alpha', 'planet:nauvis', 'ores', 'signal_type:fluid', 'signal_name:water']},
    ])
])
def test_statsd_lines_from_samples_data(test_absent_signals, expected_metrics):

    game_data = {
        'virtual_signal_names': ['signal-A'],
        'item_names': ['coal'],
        'fluid_names': ['water'],
    }

    test_samples = {
        'entities': [{
            'settings': {
                'name': 'my_metric',
                'tags': 'base=alpha,planet=nauvis,ores',
                'absent_signals': test_absent_signals,
            },
            'red_signals': [{
                'signal': {
                    'type': 'item',
                    'name': 'coal',
                },
                'count': 2,
            }],
        }],
    }

    metrics = metrics_from_samples_data(game_data, test_samples)
    assert metrics == expected_metrics


def test_statsd_packets_from_lines():
    packets = statsd_packets_from_lines(['foo', 'bar', 'baz'], 7)
    assert packets == [b'foo\nbar', b'baz']
