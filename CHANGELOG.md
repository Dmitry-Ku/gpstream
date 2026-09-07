# Changelog

All notable changes to gpstream are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this
project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `run` CLI contract per v4 §8: `--include metadata,data`, `--ddl-only` /
  `--data-only` / `--metadata-only` (F-11), `--zstd-level` (1–19),
  `--no-redistribute`, `--streams` / `--workers`.
- TOML configuration file support (`--config file.toml`, F-05) alongside YAML;
  precedence CLI > env > file > defaults.
- Strict `--include` validation: unknown object classes are rejected as typos.
- `doctor` self-diagnosis (F-16) with precise problem list.
