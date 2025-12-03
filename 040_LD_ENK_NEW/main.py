from __future__ import annotations

import argparse
from pathlib import Path

from src.pipeline import PipelineRunner


def run_for_config(config_path: Path) -> None:
    runner = PipelineRunner(config_path)
    runner.run()


def discover_machine_configs(config_dir: Path) -> list[Path]:
    configs = []
    for path in sorted(config_dir.glob("F*_Format*.ini")):
        if path.is_file():
            configs.append(path)
    return configs


def main() -> None:
    parser = argparse.ArgumentParser(description="LD-EML ENK unified pipeline")
    parser.add_argument("--config", type=Path, help="Path to a specific INI file")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run every machine INI (files matching F*_Format*.ini)",
    )
    args = parser.parse_args()

    config_dir = Path(__file__).parent / "config"
    if args.all:
        targets = discover_machine_configs(config_dir)
        if not targets:
            raise SystemExit("No machine INI files found inside config/")
        for target in targets:
            run_for_config(target)
        return

    if args.config:
        run_for_config(args.config)
        return

    parser.error("either --config or --all is required")


if __name__ == "__main__":
    main()
