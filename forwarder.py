#!/usr/bin/env python
import argparse
import itertools
import json
import logging
import os
import socket
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, Iterator, List, Optional

if TYPE_CHECKING:
    from typing_extensions import TypedDict

    class GameData(TypedDict):
        virtual_signal_names: List[str]
        item_names: List[str]
        fluid_names: List[str]

    class GameEntitySettings(TypedDict):
        name: str
        tags: str
        absent_signals: str

    class Signal(TypedDict):
        type: str
        name: str

    class CircuitSignal(TypedDict):
        signal: Signal
        count: int

    class GameEntity(TypedDict):
        settings: GameEntitySettings
        red_signals: List[CircuitSignal]
        green_signals: List[CircuitSignal]

    class SamplesData(TypedDict):
        entities: List[GameEntity]


@dataclass
class Tag:
    name: str
    value: Optional[str] = None


@dataclass
class Gauge:
    name: str
    n: int
    tags: List[Tag]


def normalize_metric_name(name: str) -> str:
    """
    Makes the name conform to common naming conventions and limitations:

        * The result will start with a letter.
        * The result will only contain alphanumerics, underscores, and periods.
        * The result will be lowercase.
        * The result will not exceed 200 characters.
    """
    if not name[0].isalpha():
        name = "x" + name
    name = name.lower()
    name = "".join(["_" if not c.isalnum() and c != "_" and c != "." else c for c in name])
    return name[:200]


def statsd_gauges_from_samples_data(
    game_data: "GameData",
    samples_data: "SamplesData",
) -> Iterator[Gauge]:
    for entity in samples_data["entities"]:
        settings = entity["settings"]
        if not settings["name"]:
            continue
        name = normalize_metric_name(settings["name"])

        gauges: Dict[str, Gauge] = {}

        tags = [Tag(*kv.split("=", 1)) for kv in settings["tags"].split(",")]

        for signal in itertools.chain(entity.get("red_signals", []), entity.get("green_signals", [])):
            signal_type_name = signal["signal"]["type"] + "." + signal["signal"]["name"]
            key = name + "|" + signal_type_name
            gauge = gauges.get(key, None)
            if gauge is None:
                gauges[key] = Gauge(
                    name=name,
                    n=signal["count"],
                    tags=tags
                    + [
                        Tag("signal_type", signal["signal"]["type"]),
                        Tag("signal_name", signal["signal"]["name"]),
                    ],
                )
            else:
                gauge.n += signal["count"]

        if settings["absent_signals"] == "treat-as-0":
            for signal_type, signal_names in [
                ("virtual", game_data["virtual_signal_names"]),
                ("item", game_data["item_names"]),
                ("fluid", game_data["fluid_names"]),
            ]:
                for signal_name in signal_names:
                    signal_type_name = signal_type + "." + signal_name
                    key = name + "|" + signal_type_name
                    if key not in gauges:

                        gauges[key] = Gauge(
                            name=name,
                            n=0,
                            tags=tags
                            + [
                                Tag("signal_type", signal_type),
                                Tag("signal_name", signal_name),
                            ],
                        )

        for gauge in gauges.values():
            yield gauge


def vanilla_line_formatter(gauge: Gauge) -> str:
    tagged_series = ";".join(itertools.chain([gauge.name], (t.name if t.value is None else f"{t.name}={t.value}" for t in gauge.tags)))
    return f"{tagged_series}:{gauge.n}|g"


def dogstatsd_line_formatter(gauge: Gauge) -> str:
    tags_str = ",".join(t.name if t.value is None else f"{t.name}:{t.value}" for t in gauge.tags)
    return f"{gauge.name}:{gauge.n}|g|#{tags_str}"


LINE_FORMATTERS = {
    "vanilla": vanilla_line_formatter,
    "dogstatsd": dogstatsd_line_formatter,
}


def statsd_packets_from_lines(lines: Iterable[str], max_size: int) -> Iterator[bytes]:
    buf = ""
    for line in lines:
        if len(buf) + 1 + len(line) > max_size:
            yield buf.encode("utf-8")
            buf = ""
        if buf:
            buf += "\n"
        buf += line
    if buf:

        yield buf.encode("utf-8")


def main() -> None:

    default_script_output = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        "script-output",
    )

    parser = argparse.ArgumentParser(description="forwards metrics from factorio to statsd")
    parser.add_argument(
        "--factorio-script-output",
        type=str,
        default=default_script_output,
        help="the path to factorio's script-output directory (default: {})".format(default_script_output),
    )
    parser.add_argument(
        "--statsd-flavor",
        type=str,
        choices=LINE_FORMATTERS.keys(),
        default=next(iter(LINE_FORMATTERS.keys())),
        help="the flavor of statsd to use",
    )
    parser.add_argument(
        "--statsd-port",
        type=int,
        default=8125,
        help="the port that statsd is listening on (default: 8125)",
    )
    parser.add_argument(
        "--statsd-host",
        type=str,
        default="127.0.0.1",
        help="the host where statsd is listening (default: 127.0.0.1)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if not os.path.exists(os.path.dirname(args.factorio_script_output)):
        logging.critical("factorio not found at script output path. please check --factorio-script-output")

    logging.info("forwarding data from " + args.factorio_script_output)

    data_path = os.path.join(args.factorio_script_output, "factorystatsd-game-data.json")
    samples_path = os.path.join(args.factorio_script_output, "factorystatsd-samples.json")

    last_game_data_mod_time = 0.0
    game_data = None

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    while True:
        try:
            if not os.path.exists(data_path):
                time.sleep(1.0)
                continue

            game_data_mod_time = os.path.getmtime(data_path)
            if game_data is None or game_data_mod_time > last_game_data_mod_time:
                with open(data_path) as f:
                    game_data = json.load(f)
                logging.info("loaded game data")
            last_game_data_mod_time = game_data_mod_time

            if not os.path.exists(samples_path):
                time.sleep(0.1)
                continue

            with open(samples_path) as f:
                samples = json.load(f)
            os.unlink(samples_path)

            line_formatter = LINE_FORMATTERS[args.statsd_flavor]
            gauges = statsd_gauges_from_samples_data(
                game_data,
                samples,
            )
            packets = list(statsd_packets_from_lines((line_formatter(g) for g in gauges), 1432))
            for packet in packets:
                sock.sendto(packet, (args.statsd_host, args.statsd_port))
            logging.info(f"sent {len(packets)} packets to statsd")
        except Exception:
            logging.exception("forwarder exception")
            time.sleep(1.0)


if __name__ == "__main__":
    main()
