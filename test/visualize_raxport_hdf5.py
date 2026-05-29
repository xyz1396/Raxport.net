import argparse
import csv
import math
import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path

import h5py
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Rectangle


MAX_PLOT_RECORDS = 8
MOBILITY_DATASETS = [
    "peaks/mobility_trace_start",
    "peaks/mobility_trace_count",
    "peak_mobility_traces/one_over_k0_index",
    "peak_mobility_traces/intensity",
    "reactions/one_over_k0_begin",
    "reactions/one_over_k0_end",
    "precursor_candidates/one_over_k0",
]
SCAN_RANGE_PATTERN = re.compile(r"\bscan=(\d+)-(\d+)\b")


@dataclass
class MobilityAxisMapper:
    indices: np.ndarray
    one_over_k0: np.ndarray

    def map(self, index_values):
        values = np.asarray(index_values, dtype=float)
        if self.indices.size < 2:
            return np.full(values.shape, np.nan, dtype=float)
        return np.interp(values, self.indices, self.one_over_k0, left=np.nan, right=np.nan)


@dataclass
class RaxportScanRecord:
    source_path: Path
    source_file: str
    source_index: int
    parent_source_index: int | None
    reaction_index: int | None
    scan_number: int
    ms_order: int
    retention_time: float
    tic: float
    scan_filter: str
    activation: str
    parent_scan_number: int
    precursor_mass: float | None
    isolation_width: float | None
    one_over_k0_begin: float | None
    one_over_k0_end: float | None
    charge_state: int | None
    collision_energy: float | None
    candidate_charge: np.ndarray
    candidate_mz: np.ndarray
    candidate_intensity: np.ndarray
    candidate_one_over_k0: np.ndarray
    peak_count: int
    parent_peak_mz: np.ndarray
    parent_peak_intensity: np.ndarray
    parent_peak_charge: np.ndarray
    parent_mobility_mz: np.ndarray
    parent_mobility_one_over_k0: np.ndarray
    parent_mobility_intensity: np.ndarray
    peak_mz: np.ndarray
    peak_intensity: np.ndarray
    peak_charge: np.ndarray
    has_mobility: bool


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Validate and visualize Raxport HDF5 output. The script checks the "
            "flat scan/peak/reaction offset schema, prints MSn precursor "
            "statistics, writes selected rows to TSV, and plots selected spectra."
        )
    )
    parser.add_argument("--input", default="data/Pan_062822_X1iso5.h5", help="Raxport HDF5 file, or a directory containing .h5 files.")
    parser.add_argument("--output", default="test/raxport_hdf5_spectra_top.pdf", help="Output plot path.")
    parser.add_argument("--tsv-output", default="", help="Output TSV path. Defaults to the plot path with .tsv suffix.")
    parser.add_argument("--top-k", type=int, default=5, help="Number of scans to select; plots at most 8.")
    parser.add_argument("--sample-size", type=int, default=1000, help="Number of candidate scans to sample before ranking. Use 0 to rank all scans.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed used when sampling scans.")
    parser.add_argument("--rank-by", choices=["tic", "peak-count", "candidate-count", "source-order", "random"], default="tic", help="How selected scans are ranked after sampling.")
    parser.add_argument("--ms-order", type=int, default=0, help="Only consider one MS order. Use 0 for all MSn scans where ms_order > 1.")
    parser.add_argument("--scan-number", action="append", type=int, default=[], help="Plot a specific scan number. Can be supplied multiple times; bypasses sampling/ranking.")
    parser.add_argument("--mz-min", type=float, default=None, help="Optional lower m/z bound for the spectrum panel.")
    parser.add_argument("--mz-max", type=float, default=None, help="Optional upper m/z bound for the spectrum panel.")
    parser.add_argument("--mz-tolerance-ppm", type=float, default=10.0, help="PPM tolerance used to match HDF5 precursor candidates back to parent MS1 peaks.")
    parser.add_argument("--show", action="store_true", help="Display the figure after saving it.")
    return parser.parse_args()


def _decode_hdf5_strings(values):
    return [_decode_hdf5_scalar_string(value) for value in values]


def _decode_hdf5_scalar_string(value):
    if isinstance(value, bytes):
        return value.decode("utf-8").rstrip("\x00")
    if hasattr(value, "item"):
        try:
            return _decode_hdf5_scalar_string(value.item())
        except ValueError:
            pass
    return str(value).rstrip("\x00")


def _read_scan_strings(scans, handle, value_name, id_name):
    if value_name in scans:
        return _decode_hdf5_strings(scans[value_name][:])
    if id_name not in scans:
        raise ValueError(f"missing scans/{value_name} or scans/{id_name}")
    table_name = f"string_tables/{value_name}"
    if table_name not in handle:
        raise ValueError(f"missing {table_name} for scans/{id_name}")
    table = _decode_hdf5_strings(handle[table_name][:])
    ids = scans[id_name][:].astype(int)
    return [table[int(item)] if 0 <= int(item) < len(table) else "" for item in ids]


def _input_hdf5_paths(path):
    if path.is_dir():
        files = sorted(item for item in path.iterdir() if item.is_file() and item.suffix.lower() in {".h5", ".hdf5"})
        if not files:
            raise ValueError(f"{path}: no .h5 or .hdf5 files found")
        return files
    if path.suffix.lower() not in {".h5", ".hdf5"}:
        raise ValueError(f"{path}: expected an HDF5 file or directory")
    return [path]


def _format_value(value):
    if value is None:
        return ""
    if isinstance(value, (float, np.floating)):
        if math.isfinite(float(value)):
            return f"{float(value):.10g}"
        return ""
    return str(value)


def _optional_float_dataset(group, name, start, stop, default=0.0):
    if name in group:
        return np.asarray(group[name][start:stop], dtype=float)
    return np.full(max(0, stop - start), default, dtype=float)


def _optional_float_value(group, name, index):
    if name not in group:
        return None
    value = float(group[name][index])
    return value if math.isfinite(value) else None


def _stats(values):
    array = np.asarray(values)
    if array.size == 0:
        return None
    return {
        "min": float(np.min(array)),
        "p25": float(np.percentile(array, 25)),
        "median": float(np.median(array)),
        "mean": float(np.mean(array)),
        "p75": float(np.percentile(array, 75)),
        "max": float(np.max(array)),
    }


def _stats_text(name, values):
    stats = _stats(values)
    if stats is None:
        return f"{name}=none"
    return (
        f"{name}_min={_format_value(stats['min'])}\t"
        f"{name}_p25={_format_value(stats['p25'])}\t"
        f"{name}_median={_format_value(stats['median'])}\t"
        f"{name}_mean={_format_value(stats['mean'])}\t"
        f"{name}_p75={_format_value(stats['p75'])}\t"
        f"{name}_max={_format_value(stats['max'])}"
    )


def _has_mobility(handle):
    if not all(name in handle for name in MOBILITY_DATASETS):
        return False
    reaction_values = np.concatenate((
        np.asarray(handle["reactions/one_over_k0_begin"][:], dtype=float),
        np.asarray(handle["reactions/one_over_k0_end"][:], dtype=float),
    ))
    if np.any(np.isfinite(reaction_values) & (reaction_values != 0.0)):
        return True
    candidate_values = np.asarray(handle["precursor_candidates/one_over_k0"][:], dtype=float)
    return bool(np.any(np.isfinite(candidate_values) & (candidate_values != 0.0)))


def _validate_required_datasets(handle, path):
    required = [
        "scans/scan_number", "scans/ms_order", "scans/retention_time", "scans/tic", "scans/parent_scan_number",
        "scans/reaction_start", "scans/reaction_count", "scans/peak_start", "scans/peak_count",
        "peaks/mz", "peaks/intensity", "peaks/charge",
        "reactions/precursor_mass", "reactions/isolation_width", "reactions/charge_state", "reactions/collision_energy",
        "reactions/candidate_start", "reactions/candidate_count", "precursor_candidates/charge", "precursor_candidates/mz",
    ]
    if "scans/scan_filter" not in handle:
        required.extend(["scans/scan_filter_id", "string_tables/scan_filter"])
    if "scans/activation" not in handle:
        required.extend(["scans/activation_id", "string_tables/activation"])
    missing = [name for name in required if name not in handle]
    if missing:
        raise ValueError(f"{path}: missing required datasets: {', '.join(missing)}")


def _validate_offsets(handle, path):
    scan_count = handle["scans/scan_number"].shape[0]
    scan_datasets = ["ms_order", "retention_time", "tic", "parent_scan_number", "reaction_start", "reaction_count", "peak_start", "peak_count"]
    scan_datasets.append("scan_filter" if "scan_filter" in handle["scans"] else "scan_filter_id")
    scan_datasets.append("activation" if "activation" in handle["scans"] else "activation_id")
    for name in scan_datasets:
        count = handle[f"scans/{name}"].shape[0]
        if count != scan_count:
            raise ValueError(f"{path}: scans/{name} has {count} rows, expected {scan_count}")

    peak_total = handle["peaks/mz"].shape[0]
    peak_start = handle["scans/peak_start"][:].astype(np.int64)
    peak_count = handle["scans/peak_count"][:].astype(np.int64)
    if peak_start.size and int(np.max(peak_start + peak_count)) > peak_total:
        raise ValueError(f"{path}: scan peak offsets exceed /peaks row count")

    reaction_total = handle["reactions/precursor_mass"].shape[0]
    reaction_start = handle["scans/reaction_start"][:].astype(np.int64)
    reaction_count = handle["scans/reaction_count"][:].astype(np.int64)
    reaction_mask = reaction_count > 0
    if reaction_mask.any() and int(np.max(reaction_start[reaction_mask] + reaction_count[reaction_mask])) > reaction_total:
        raise ValueError(f"{path}: scan reaction offsets exceed /reactions row count")

    candidate_total = handle["precursor_candidates/mz"].shape[0]
    candidate_start = handle["reactions/candidate_start"][:].astype(np.int64)
    candidate_count = handle["reactions/candidate_count"][:].astype(np.int64)
    if candidate_start.size and int(np.max(candidate_start + candidate_count)) > candidate_total:
        raise ValueError(f"{path}: reaction candidate offsets exceed /precursor_candidates row count")

    if _has_mobility(handle):
        trace_total = handle["peak_mobility_traces/intensity"].shape[0]
        trace_start = handle["peaks/mobility_trace_start"][:].astype(np.int64)
        trace_count = handle["peaks/mobility_trace_count"][:].astype(np.int64)
        trace_mask = trace_count > 0
        if trace_mask.any() and int(np.max(trace_start[trace_mask] + trace_count[trace_mask])) > trace_total:
            raise ValueError(f"{path}: peak mobility trace offsets exceed /peak_mobility_traces row count")


def validate_hdf5_file(path):
    with h5py.File(path, "r") as handle:
        _validate_required_datasets(handle, path)
        _validate_offsets(handle, path)


def _build_mobility_axis_mapper(handle, scan_filter, ms_order, reaction_start, reaction_count):
    if not _has_mobility(handle):
        return None
    values_by_index = defaultdict(list)
    begin_values = handle["reactions/one_over_k0_begin"]
    end_values = handle["reactions/one_over_k0_end"]
    for scan_index, text in enumerate(scan_filter):
        if ms_order[scan_index] <= 1 or reaction_count[scan_index] <= 0:
            continue
        match = SCAN_RANGE_PATTERN.search(text)
        if not match:
            continue
        begin_index = int(match.group(1))
        end_index = int(match.group(2))
        r_index = int(reaction_start[scan_index])
        begin = float(begin_values[r_index])
        end = float(end_values[r_index])
        if not (math.isfinite(begin) and math.isfinite(end)) or (begin == 0.0 and end == 0.0):
            continue
        values_by_index[begin_index].append(begin)
        values_by_index[end_index].append(end)
    if len(values_by_index) < 2:
        return None
    indices = np.asarray(sorted(values_by_index), dtype=float)
    one_over_k0 = np.asarray([float(np.median(values_by_index[int(index)])) for index in indices], dtype=float)
    order = np.argsort(indices)
    return MobilityAxisMapper(indices=indices[order], one_over_k0=one_over_k0[order])


def load_hdf5_records(path, ms_order_filter=0):
    records = []
    with h5py.File(path, "r") as handle:
        _validate_required_datasets(handle, path)
        _validate_offsets(handle, path)
        scans = handle["scans"]
        reactions = handle["reactions"]
        candidates = handle["precursor_candidates"]
        has_mobility = _has_mobility(handle)

        scan_number = scans["scan_number"][:].astype(int)
        ms_order = scans["ms_order"][:].astype(int)
        retention_time = scans["retention_time"][:].astype(float)
        tic = scans["tic"][:].astype(float)
        scan_filter = _read_scan_strings(scans, handle, "scan_filter", "scan_filter_id")
        activation = _read_scan_strings(scans, handle, "activation", "activation_id")
        parent_scan_number = scans["parent_scan_number"][:].astype(int)
        reaction_start = scans["reaction_start"][:].astype(np.int64)
        reaction_count = scans["reaction_count"][:].astype(np.int64)
        peak_count = scans["peak_count"][:].astype(np.int64)
        scan_index_by_number = {int(number): index for index, number in enumerate(scan_number)}

        precursor_mass = reactions["precursor_mass"][:].astype(float)
        isolation_width = reactions["isolation_width"][:].astype(float)
        charge_state = reactions["charge_state"][:].astype(int)
        collision_energy = reactions["collision_energy"][:].astype(float)
        reaction_one_over_k0_begin = reactions["one_over_k0_begin"][:].astype(float) if "one_over_k0_begin" in reactions else None
        reaction_one_over_k0_end = reactions["one_over_k0_end"][:].astype(float) if "one_over_k0_end" in reactions else None
        candidate_start = reactions["candidate_start"][:].astype(np.int64)
        candidate_count = reactions["candidate_count"][:].astype(np.int64)
        candidate_charge = candidates["charge"][:].astype(int)
        candidate_mz = candidates["mz"][:].astype(float)
        candidate_intensity = candidates["intensity"][:].astype(float) if "intensity" in candidates else np.zeros(candidate_mz.shape[0], dtype=float)
        candidate_one_over_k0 = candidates["one_over_k0"][:].astype(float) if "one_over_k0" in candidates else np.full(candidate_mz.shape[0], np.nan, dtype=float)

        for index in range(len(scan_number)):
            if ms_order[index] <= 1:
                continue
            if ms_order_filter > 0 and ms_order[index] != ms_order_filter:
                continue

            precursor = width = one_over_k0_begin = one_over_k0_end = charge = energy = reaction_index = None
            cand_charge_array = np.asarray([], dtype=int)
            cand_mz_array = np.asarray([], dtype=float)
            cand_intensity_array = np.asarray([], dtype=float)
            cand_one_over_k0_array = np.asarray([], dtype=float)
            if reaction_count[index] > 0:
                reaction_index = int(reaction_start[index])
                precursor = float(precursor_mass[reaction_index])
                width = float(isolation_width[reaction_index])
                if reaction_one_over_k0_begin is not None:
                    value = float(reaction_one_over_k0_begin[reaction_index])
                    one_over_k0_begin = value if math.isfinite(value) else None
                if reaction_one_over_k0_end is not None:
                    value = float(reaction_one_over_k0_end[reaction_index])
                    one_over_k0_end = value if math.isfinite(value) else None
                charge = int(charge_state[reaction_index])
                energy = float(collision_energy[reaction_index])
                c_start = int(candidate_start[reaction_index])
                c_stop = c_start + int(candidate_count[reaction_index])
                cand_charge_array = np.asarray(candidate_charge[c_start:c_stop], dtype=int)
                cand_mz_array = np.asarray(candidate_mz[c_start:c_stop], dtype=float)
                cand_intensity_array = np.asarray(candidate_intensity[c_start:c_stop], dtype=float)
                cand_one_over_k0_array = np.asarray(candidate_one_over_k0[c_start:c_stop], dtype=float)

            parent_index = scan_index_by_number.get(int(parent_scan_number[index]))
            records.append(RaxportScanRecord(
                source_path=Path(path), source_file=Path(path).name, source_index=index, parent_source_index=parent_index,
                reaction_index=reaction_index, scan_number=int(scan_number[index]), ms_order=int(ms_order[index]),
                retention_time=float(retention_time[index]), tic=float(tic[index]), scan_filter=scan_filter[index],
                activation=activation[index], parent_scan_number=int(parent_scan_number[index]), precursor_mass=precursor,
                isolation_width=width, one_over_k0_begin=one_over_k0_begin, one_over_k0_end=one_over_k0_end,
                charge_state=charge, collision_energy=energy, candidate_charge=cand_charge_array, candidate_mz=cand_mz_array,
                candidate_intensity=cand_intensity_array, candidate_one_over_k0=cand_one_over_k0_array, peak_count=int(peak_count[index]),
                parent_peak_mz=np.asarray([], dtype=float), parent_peak_intensity=np.asarray([], dtype=float), parent_peak_charge=np.asarray([], dtype=int),
                parent_mobility_mz=np.asarray([], dtype=float), parent_mobility_one_over_k0=np.asarray([], dtype=float), parent_mobility_intensity=np.asarray([], dtype=float),
                peak_mz=np.asarray([], dtype=float), peak_intensity=np.asarray([], dtype=float), peak_charge=np.asarray([], dtype=int), has_mobility=has_mobility,
            ))
    return records


def load_records(path, ms_order_filter=0):
    records = []
    for hdf5_path in _input_hdf5_paths(path):
        records.extend(load_hdf5_records(hdf5_path, ms_order_filter))
    return records


def _load_peak_slice(handle, scan_index):
    start = int(handle["scans/peak_start"][scan_index])
    stop = start + int(handle["scans/peak_count"][scan_index])
    peaks = handle["peaks"]
    return (
        np.asarray(peaks["mz"][start:stop], dtype=float),
        np.asarray(peaks["intensity"][start:stop], dtype=float),
        np.asarray(peaks["charge"][start:stop], dtype=int),
        start,
        stop,
    )


def _load_parent_mobility(handle, parent_peak_mz, parent_peak_start, parent_peak_stop, mapper):
    if mapper is None or parent_peak_mz.size == 0:
        empty = np.asarray([], dtype=float)
        return empty, empty, empty
    starts = np.asarray(handle["peaks/mobility_trace_start"][parent_peak_start:parent_peak_stop], dtype=np.int64)
    counts = np.asarray(handle["peaks/mobility_trace_count"][parent_peak_start:parent_peak_stop], dtype=np.int64)
    valid = (starts >= 0) & (counts > 0)
    if not valid.any():
        empty = np.asarray([], dtype=float)
        return empty, empty, empty
    valid_starts = starts[valid]
    valid_counts = counts[valid]
    span_start = int(np.min(valid_starts))
    span_stop = int(np.max(valid_starts + valid_counts))
    trace_indices = np.asarray(handle["peak_mobility_traces/one_over_k0_index"][span_start:span_stop], dtype=int)
    trace_intensity = np.asarray(handle["peak_mobility_traces/intensity"][span_start:span_stop], dtype=float)
    total = int(np.sum(valid_counts))
    mobility_mz = np.empty(total, dtype=float)
    mobility_index = np.empty(total, dtype=int)
    mobility_intensity = np.empty(total, dtype=float)
    out = 0
    valid_peak_indices = np.flatnonzero(valid)
    for peak_index, start, count in zip(valid_peak_indices, valid_starts, valid_counts):
        count = int(count)
        rel_start = int(start) - span_start
        rel_stop = rel_start + count
        next_out = out + count
        mobility_mz[out:next_out] = float(parent_peak_mz[peak_index])
        mobility_index[out:next_out] = trace_indices[rel_start:rel_stop]
        mobility_intensity[out:next_out] = trace_intensity[rel_start:rel_stop]
        out = next_out
    mobility_one_over_k0 = mapper.map(mobility_index)
    finite = np.isfinite(mobility_one_over_k0) & np.isfinite(mobility_intensity)
    return mobility_mz[finite], mobility_one_over_k0[finite], mobility_intensity[finite]


def hydrate_records(records):
    by_path = defaultdict(list)
    for record in records:
        by_path[record.source_path].append(record)
    for path, path_records in by_path.items():
        with h5py.File(path, "r") as handle:
            scans = handle["scans"]
            scan_filter = _read_scan_strings(scans, handle, "scan_filter", "scan_filter_id")
            ms_order = scans["ms_order"][:].astype(int)
            reaction_start = scans["reaction_start"][:].astype(np.int64)
            reaction_count = scans["reaction_count"][:].astype(np.int64)
            mapper = _build_mobility_axis_mapper(handle, scan_filter, ms_order, reaction_start, reaction_count)
            for record in path_records:
                if record.parent_source_index is None:
                    parent_peak_mz = np.asarray([], dtype=float)
                    parent_peak_intensity = np.asarray([], dtype=float)
                    parent_peak_charge = np.asarray([], dtype=int)
                    parent_start = parent_stop = 0
                else:
                    parent_peak_mz, parent_peak_intensity, parent_peak_charge, parent_start, parent_stop = _load_peak_slice(handle, record.parent_source_index)
                peak_mz, peak_intensity, peak_charge, _, _ = _load_peak_slice(handle, record.source_index)
                record.parent_peak_mz = parent_peak_mz
                record.parent_peak_intensity = parent_peak_intensity
                record.parent_peak_charge = parent_peak_charge
                record.peak_mz = peak_mz
                record.peak_intensity = peak_intensity
                record.peak_charge = peak_charge
                if record.has_mobility and mapper is not None:
                    record.parent_mobility_mz, record.parent_mobility_one_over_k0, record.parent_mobility_intensity = _load_parent_mobility(
                        handle, parent_peak_mz, parent_start, parent_stop, mapper
                    )
    return records


def _sample_records(records, sample_size, seed):
    if sample_size == 0 or len(records) <= sample_size:
        return list(records)
    return random.Random(seed).sample(records, sample_size)


def _select_records(records, args):
    if args.scan_number:
        by_scan = {record.scan_number: record for record in records}
        missing = [str(scan) for scan in args.scan_number if scan not in by_scan]
        if missing:
            raise SystemExit(f"Scan number not found among selected MSn scans: {', '.join(missing)}")
        return [by_scan[scan] for scan in args.scan_number]
    sampled = _sample_records(records, args.sample_size, args.seed)
    if args.rank_by == "random":
        random.Random(args.seed).shuffle(sampled)
        return sampled[: args.top_k]
    if args.rank_by == "source-order":
        ranked = sorted(sampled, key=lambda record: record.source_index)
    elif args.rank_by == "peak-count":
        ranked = sorted(sampled, key=lambda record: (record.peak_count, record.tic), reverse=True)
    elif args.rank_by == "candidate-count":
        ranked = sorted(sampled, key=lambda record: (record.candidate_mz.size, record.tic), reverse=True)
    else:
        ranked = sorted(sampled, key=lambda record: (record.tic, record.peak_count), reverse=True)
    return ranked[: args.top_k]


def _set_mz_xlim(ax, mz_values, mz_min=None, mz_max=None):
    if mz_min is not None or mz_max is not None:
        if mz_min is None:
            mz_min = float(np.min(mz_values)) if mz_values.size else 0.0
        if mz_max is None:
            mz_max = float(np.max(mz_values)) if mz_values.size else mz_min + 1.0
        ax.set_xlim(mz_min, mz_max)
        return
    if mz_values.size == 0:
        ax.set_xlim(0.0, 1.0)
        return
    min_mz = float(np.min(mz_values))
    max_mz = float(np.max(mz_values))
    pad = max(1.0, 0.02 * max(1.0, max_mz - min_mz))
    ax.set_xlim(min_mz - pad, max_mz + pad)


def _mz_xlim_values(mz_values, mz_min=None, mz_max=None):
    mz_values = np.asarray(mz_values, dtype=float)
    finite = mz_values[np.isfinite(mz_values)]
    if mz_min is not None or mz_max is not None:
        if mz_min is None:
            mz_min = float(np.min(finite)) if finite.size else 0.0
        if mz_max is None:
            mz_max = float(np.max(finite)) if finite.size else mz_min + 1.0
        return float(mz_min), float(mz_max)
    if finite.size == 0:
        return 0.0, 1.0
    min_mz = float(np.min(finite))
    max_mz = float(np.max(finite))
    pad = max(1.0, 0.02 * max(1.0, max_mz - min_mz))
    return min_mz - pad, max_mz + pad


def _apply_shared_x(ax_top, ax_bottom, xlim):
    ax_top.set_xlim(*xlim)
    ax_bottom.set_xlim(*xlim)
    ax_top.xaxis.set_major_locator(ax_bottom.xaxis.get_major_locator())
    ax_top.xaxis.set_major_formatter(ax_bottom.xaxis.get_major_formatter())
    ax_top.tick_params(axis="x", labelbottom=False)


def _add_intensity_colorbar(fig, scatter, ax, cax=None):
    if cax is None:
        return fig.colorbar(scatter, ax=ax, fraction=0.046, pad=0.02, label="MS1 intensity")
    return fig.colorbar(scatter, cax=cax, label="MS1 intensity")


def _hide_spacer_axis(ax):
    ax.set_visible(False)
    ax.set_axis_off()


def _mobility_window_bounds(record):
    if record is None or record.one_over_k0_begin is None or record.one_over_k0_end is None:
        return None, None
    lower = min(record.one_over_k0_begin, record.one_over_k0_end)
    upper = max(record.one_over_k0_begin, record.one_over_k0_end)
    if lower == 0.0 and upper == 0.0:
        return None, None
    return lower, upper


def _set_k0_ylim(ax, k0_values, record=None, zoom=False):
    finite = np.asarray(k0_values, dtype=float)
    finite = finite[np.isfinite(finite)]
    bounds = _mobility_window_bounds(record) if record is not None else (None, None)
    if zoom and bounds[0] is not None:
        lower, upper = bounds
        pad = max(0.01, abs(upper - lower) * 0.35)
        ax.set_ylim(lower - pad, upper + pad)
        return
    if finite.size == 0:
        ax.set_ylim(0.0, 1.0)
        return
    lower = float(np.min(finite))
    upper = float(np.max(finite))
    pad = max(0.01, 0.03 * max(0.01, upper - lower))
    ax.set_ylim(lower - pad, upper + pad)


def _plot_peak_series(ax, mz, intensity, color, label, linewidth=0.4, alpha=0.65):
    if mz.size:
        ax.vlines(mz, 0.0, intensity, color=color, alpha=alpha, linewidth=linewidth, label=label)


def _dedup_legend(ax, loc="upper right"):
    handles, labels = ax.get_legend_handles_labels()
    dedup = {}
    for handle, label in zip(handles, labels):
        dedup.setdefault(label, handle)
    if dedup:
        ax.legend(dedup.values(), dedup.keys(), loc=loc, fontsize=8)


def _normalize_intensity(intensity):
    if intensity.size and float(np.max(intensity)) > 0.0:
        return intensity / float(np.max(intensity))
    return intensity


def _mz_mask(mz, mz_min=None, mz_max=None):
    mask = np.ones(mz.shape[0], dtype=bool)
    if mz_min is not None:
        mask &= mz >= mz_min
    if mz_max is not None:
        mask &= mz <= mz_max
    return mask


def _candidate_zoom_bounds(record):
    if record.precursor_mass is not None and record.isolation_width is not None:
        half_width = max(record.isolation_width / 2.0, 0.5)
        pad = max(1.0, half_width * 0.35)
        return record.precursor_mass - half_width - pad, record.precursor_mass + half_width + pad
    if record.candidate_mz.size:
        min_mz = float(np.min(record.candidate_mz))
        max_mz = float(np.max(record.candidate_mz))
        pad = max(1.0, 0.15 * max(1.0, max_mz - min_mz))
        return min_mz - pad, max_mz + pad
    return None, None


def _isolation_mz_bounds(record):
    if record.precursor_mass is None or record.isolation_width is None:
        return None, None
    half_width = record.isolation_width / 2.0
    return record.precursor_mass - half_width, record.precursor_mass + half_width


def _nearest_peak_index(mz_values, target_mz):
    if mz_values.size == 0:
        return -1
    return int(np.argmin(np.abs(mz_values - target_mz)))


def _mz_tolerance_da(mz, tolerance_ppm):
    return abs(float(mz)) * tolerance_ppm / 1_000_000.0


def _delta_ppm(observed, expected):
    if expected == 0:
        return math.inf
    return (float(observed) - float(expected)) / float(expected) * 1_000_000.0


def _nearest_peak_index_within_tolerance(mz_values, target_mz, tolerance_ppm):
    peak_index = _nearest_peak_index(mz_values, target_mz)
    if peak_index < 0:
        return -1
    if abs(float(mz_values[peak_index]) - float(target_mz)) <= _mz_tolerance_da(target_mz, tolerance_ppm):
        return peak_index
    return -1


def _matched_parent_peak_indices(record, tolerance_ppm):
    return [_nearest_peak_index_within_tolerance(record.parent_peak_mz, float(candidate_mz), tolerance_ppm) for candidate_mz in record.candidate_mz]


def _unique_candidate_indices(record):
    seen = set()
    indices = []
    for index, candidate_mz in enumerate(record.candidate_mz):
        key = round(float(candidate_mz), 6)
        if key in seen:
            continue
        seen.add(key)
        indices.append(index)
    return indices


def _candidate_charges_for_mz(record, target_mz):
    charges = []
    for candidate_mz, charge in zip(record.candidate_mz, record.candidate_charge):
        if round(float(candidate_mz), 6) == round(float(target_mz), 6):
            charges.append(int(charge))
    return sorted(set(charges))


def _top_candidate_one_over_k0(record):
    for index in _unique_candidate_indices(record):
        if index < record.candidate_one_over_k0.size:
            value = float(record.candidate_one_over_k0[index])
            if math.isfinite(value) and value != 0.0:
                return value
    return None


def _top_precursor_text(record):
    unique_indices = _unique_candidate_indices(record)
    if not unique_indices:
        top_mz = "NA"
        top_charge = "NA"
    else:
        top_index = unique_indices[0]
        top_mz_value = float(record.candidate_mz[top_index])
        top_mz = _format_value(top_mz_value)
        top_charge = ",".join(str(charge) for charge in _candidate_charges_for_mz(record, top_mz_value))
    k0 = _top_candidate_one_over_k0(record)
    k0_text = f" | precursor 1/k0={_format_value(k0)}" if k0 is not None else ""
    return f"top precursor m/z={top_mz} z={top_charge}{k0_text}\nisolation center={_format_value(record.precursor_mass)} | precursor count={len(unique_indices)}"



def _compact_precursor_text(record):
    unique_indices = _unique_candidate_indices(record)
    if not unique_indices:
        top_mz = "NA"
        top_charge = "NA"
    else:
        top_index = unique_indices[0]
        top_mz_value = float(record.candidate_mz[top_index])
        top_mz = _format_value(top_mz_value)
        top_charge = ",".join(str(charge) for charge in _candidate_charges_for_mz(record, top_mz_value))
    k0 = _top_candidate_one_over_k0(record)
    parts = [f"top m/z={top_mz}", f"z={top_charge}"]
    if k0 is not None:
        parts.append(f"1/k0={_format_value(k0)}")
    parts.append(f"n={len(unique_indices)}")
    return " | ".join(parts)


def _compact_msn_title(record):
    return (
        f"{record.source_file}\n"
        f"scan={record.scan_number} | MS{record.ms_order} | RT={record.retention_time:.4f} | "
        f"parent={record.parent_scan_number} | precursor={_format_value(record.precursor_mass)} | "
        f"z={record.charge_state or 'NA'} | candidates={record.candidate_mz.size} | peaks={record.peak_mz.size}"
    )


def _draw_precursor_markers(ax, record, tolerance_ppm, plotted_mz=None, plotted_intensity=None, max_arrows=None):
    if record.precursor_mass is not None:
        ax.axvline(record.precursor_mass, color="tab:red", linestyle="--", linewidth=1.0, alpha=0.85, label="Isolation center")
        if record.isolation_width is not None:
            half_width = record.isolation_width / 2.0
            ax.axvspan(record.precursor_mass - half_width, record.precursor_mass + half_width, color="tab:red", alpha=0.08, label="Isolation window")
    if record.candidate_mz.size == 0:
        return
    mz_values = record.parent_peak_mz if plotted_mz is None else plotted_mz
    intensity_values = _normalize_intensity(record.parent_peak_intensity) if plotted_intensity is None else plotted_intensity
    matched_parent_indices = _matched_parent_peak_indices(record, tolerance_ppm)
    unique_candidate_indices = _unique_candidate_indices(record)
    if max_arrows is not None:
        unique_candidate_indices = unique_candidate_indices[:max_arrows]
    arrow_count = 0
    used_parent_peak_indices = set()
    for candidate_index in unique_candidate_indices:
        parent_peak_index = matched_parent_indices[candidate_index]
        if parent_peak_index < 0 or parent_peak_index in used_parent_peak_indices:
            continue
        used_parent_peak_indices.add(parent_peak_index)
        parent_peak_mz = float(record.parent_peak_mz[parent_peak_index])
        plotted_peak_index = _nearest_peak_index_within_tolerance(mz_values, parent_peak_mz, tolerance_ppm)
        if plotted_peak_index < 0:
            continue
        peak_mz = float(mz_values[plotted_peak_index])
        peak_y = float(intensity_values[plotted_peak_index]) if intensity_values.size else 0.0
        ax.vlines([peak_mz], 0.0, [peak_y], color="tab:red", linewidth=1.0, alpha=0.95, label="Precursor peak" if arrow_count == 0 else None)
        ax.annotate("", xy=(peak_mz, min(1.03, max(0.04, peak_y + 0.025))), xytext=(peak_mz, min(1.1, max(0.16, peak_y + 0.14))), arrowprops={"arrowstyle": "-|>", "color": "tab:orange", "linewidth": 1.0, "alpha": 0.9, "shrinkA": 0, "shrinkB": 0})
        arrow_count += 1
    if arrow_count:
        ax.plot([], [], color="tab:orange", linewidth=1.2, label="Top precursor" if max_arrows == 1 else "Top precursor candidates")


def _draw_unselected_parent_peaks(ax, record, tolerance_ppm, plotted_mz, plotted_intensity, max_arrows=None):
    if plotted_mz.size == 0:
        return
    matched_parent_indices = set(_matched_parent_peak_indices(record, tolerance_ppm))
    matched_parent_indices.discard(-1)
    if max_arrows is not None:
        parent_indices = _matched_parent_peak_indices(record, tolerance_ppm)
        matched_parent_indices = set(parent_indices[index] for index in _unique_candidate_indices(record)[:max_arrows])
        matched_parent_indices.discard(-1)
    unselected_mz = []
    unselected_intensity = []
    for mz, intensity in zip(plotted_mz, plotted_intensity):
        parent_index = _nearest_peak_index_within_tolerance(record.parent_peak_mz, float(mz), tolerance_ppm)
        if parent_index not in matched_parent_indices:
            unselected_mz.append(float(mz))
            unselected_intensity.append(float(intensity))
    if unselected_mz:
        ax.vlines(np.asarray(unselected_mz), 0.0, np.asarray(unselected_intensity), color="tab:green", linewidth=0.45, alpha=0.9, label="Unselected parent peaks")


def _draw_isolation_rectangle(ax, record):
    mz_min, mz_max = _isolation_mz_bounds(record)
    k0_min, k0_max = _mobility_window_bounds(record)
    if mz_min is None or k0_min is None:
        return
    ax.add_patch(Rectangle((mz_min, k0_min), mz_max - mz_min, k0_max - k0_min, fill=False, edgecolor="tab:red", linewidth=1.2, linestyle="--", label="Isolation window"))


def _positive_intensity_norm(intensity):
    positive = intensity[np.isfinite(intensity) & (intensity > 0)]
    if positive.size == 0:
        return None
    vmin = max(float(np.min(positive)), 1e-6)
    vmax = float(np.max(positive))
    if vmax <= vmin:
        vmax = vmin * 10.0
    return mcolors.LogNorm(vmin=vmin, vmax=vmax)


def _plot_mobility_points(ax, record, mz_min=None, mz_max=None, k0_zoom=False):
    mz = record.parent_mobility_mz
    k0 = record.parent_mobility_one_over_k0
    intensity = record.parent_mobility_intensity
    mask = np.isfinite(mz) & np.isfinite(k0) & np.isfinite(intensity) & (intensity > 0)
    if mz_min is not None:
        mask &= mz >= mz_min
    if mz_max is not None:
        mask &= mz <= mz_max
    if k0_zoom:
        k0_min, k0_max = _mobility_window_bounds(record)
        if k0_min is not None:
            pad = max(0.01, abs(k0_max - k0_min) * 0.35)
            mask &= (k0 >= k0_min - pad) & (k0 <= k0_max + pad)
    if not mask.any():
        ax.text(0.5, 0.5, "No mapped MS1 mobility points", transform=ax.transAxes, ha="center", va="center")
        return None
    scatter = ax.scatter(mz[mask], k0[mask], c=intensity[mask], s=2.0, cmap="viridis", norm=_positive_intensity_norm(intensity[mask]), linewidths=0, alpha=0.75, rasterized=True)
    return scatter


def _candidate_mobility_marker_points(record):
    points = []
    for candidate_index in _unique_candidate_indices(record):
        if candidate_index >= record.candidate_one_over_k0.size:
            continue
        marker_mz = float(record.candidate_mz[candidate_index])
        parent_index = _nearest_peak_index(record.parent_peak_mz, marker_mz)
        if parent_index >= 0:
            marker_mz = float(record.parent_peak_mz[parent_index])
        marker_k0 = float(record.candidate_one_over_k0[candidate_index])
        points.append((candidate_index, marker_mz, marker_k0))
    return points


def _draw_candidate_mobility_markers(ax, record, annotate=False, max_annotations=1):
    drawn = 0
    annotated = 0
    for candidate_index, mz, k0 in _candidate_mobility_marker_points(record):
        if not (math.isfinite(mz) and math.isfinite(k0)) or k0 == 0.0:
            continue
        ax.scatter([mz], [k0], marker="x", s=34, color="tab:orange", linewidths=1.3, label="Precursor candidates" if drawn == 0 else None, zorder=5)
        if annotate and annotated < max_annotations:
            charges = ",".join(str(charge) for charge in _candidate_charges_for_mz(record, mz))
            ax.annotate(
                f"top m/z {_format_value(mz)}\nz={charges or 'NA'} | 1/k0 {_format_value(k0)}",
                xy=(mz, k0),
                xytext=(8, 8),
                textcoords="offset points",
                fontsize=7,
                color="tab:orange",
                bbox={"boxstyle": "round,pad=0.18", "fc": "white", "ec": "tab:orange", "alpha": 0.82, "lw": 0.4},
                arrowprops={"arrowstyle": "-", "color": "tab:orange", "linewidth": 0.6, "alpha": 0.8},
            )
            annotated += 1
        drawn += 1


def _plot_parent_mobility(ax, record, fig, cax=None):
    scatter = _plot_mobility_points(ax, record)
    _draw_isolation_rectangle(ax, record)
    _draw_candidate_mobility_markers(ax, record)
    _set_mz_xlim(ax, record.parent_mobility_mz if record.parent_mobility_mz.size else record.parent_peak_mz)
    _set_k0_ylim(ax, record.parent_mobility_one_over_k0, record=record)
    ax.set_xlabel("")
    ax.tick_params(axis="x", labelbottom=False)
    ax.set_ylabel("1/k0")
    ax.set_title(f"Parent MS1 mobility map {record.parent_scan_number}\n{_compact_precursor_text(record)}", fontsize=8, pad=3)
    if scatter is not None:
        _add_intensity_colorbar(fig, scatter, ax, cax=cax)
    elif cax is not None:
        _hide_spacer_axis(cax)
    _dedup_legend(ax)


def _plot_parent_mobility_zoom(ax, record, fig, cax=None):
    mz_min, mz_max = _candidate_zoom_bounds(record)
    scatter = _plot_mobility_points(ax, record, mz_min=mz_min, mz_max=mz_max, k0_zoom=True)
    _draw_isolation_rectangle(ax, record)
    _draw_candidate_mobility_markers(ax, record, annotate=True)
    _set_mz_xlim(ax, record.parent_mobility_mz if record.parent_mobility_mz.size else record.candidate_mz, mz_min, mz_max)
    _set_k0_ylim(ax, record.parent_mobility_one_over_k0, record=record, zoom=True)
    k0 = _top_candidate_one_over_k0(record)
    title = "Parent 2D zoom: isolation window"
    if k0 is not None:
        title += f" | precursor 1/k0={_format_value(k0)}"
    ax.set_title(title, fontsize=8, pad=3)
    ax.set_xlabel("")
    ax.tick_params(axis="x", labelbottom=False)
    ax.set_ylabel("1/k0")
    if scatter is not None:
        _add_intensity_colorbar(fig, scatter, ax, cax=cax)
    elif cax is not None:
        _hide_spacer_axis(cax)
    _dedup_legend(ax, loc="upper left")


def _plot_parent(ax, record, tolerance_ppm, mz_xlim=None):
    intensity = _normalize_intensity(record.parent_peak_intensity)
    _draw_precursor_markers(ax, record, tolerance_ppm, record.parent_peak_mz, intensity, max_arrows=1)
    _draw_unselected_parent_peaks(ax, record, tolerance_ppm, record.parent_peak_mz, intensity, max_arrows=1)
    if mz_xlim is None:
        _set_mz_xlim(ax, record.parent_peak_mz)
    else:
        ax.set_xlim(*mz_xlim)
    ax.set_ylim(0.0, 1.12)
    ax.set_xlabel("m/z")
    ax.set_ylabel("Relative intensity")
    ax.set_title(f"Parent MS1 scan {record.parent_scan_number}\n{_compact_precursor_text(record)}", fontsize=8, pad=3)
    _dedup_legend(ax)


def _plot_parent_zoom(ax, record, tolerance_ppm, mz_xlim=None):
    if mz_xlim is None:
        mz_min, mz_max = _candidate_zoom_bounds(record)
    else:
        mz_min, mz_max = mz_xlim
    mask = _mz_mask(record.parent_peak_mz, mz_min, mz_max)
    mz = record.parent_peak_mz[mask]
    intensity = _normalize_intensity(record.parent_peak_intensity[mask])
    _draw_precursor_markers(ax, record, tolerance_ppm, mz, intensity)
    _draw_unselected_parent_peaks(ax, record, tolerance_ppm, mz, intensity)
    if mz_xlim is None:
        _set_mz_xlim(ax, mz if mz.size else record.candidate_mz, mz_min, mz_max)
    else:
        ax.set_xlim(*mz_xlim)
    ax.set_ylim(0.0, 1.12)
    ax.set_xlabel("m/z")
    ax.set_ylabel("Relative intensity")
    title = "Parent zoom: candidate precursors"
    k0 = _top_candidate_one_over_k0(record)
    if k0 is not None:
        title += f" | precursor 1/k0={_format_value(k0)}"
    ax.set_title(title, fontsize=8, pad=3)
    _dedup_legend(ax)


def _plot_msn(ax, record, mz_min=None, mz_max=None):
    mask = _mz_mask(record.peak_mz, mz_min, mz_max)
    mz = record.peak_mz[mask]
    intensity = _normalize_intensity(record.peak_intensity[mask])
    _plot_peak_series(ax, mz, intensity, color="tab:blue", label="MSn peaks")
    _set_mz_xlim(ax, mz, mz_min, mz_max)
    ax.set_ylim(0.0, 1.08)
    ax.set_xlabel("m/z")
    ax.set_ylabel("Relative intensity")
    ax.set_title(_compact_msn_title(record), fontsize=8, pad=3)
    _dedup_legend(ax)


def _figure_fraction_for_inches(inches, total_inches):
    return min(0.98, max(0.0, inches / max(total_inches, 1.0)))


def _plot_records(records, input_path, output_path, args):
    plotted = records[: min(MAX_PLOT_RECORDS, len(records))]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    any_mobility = any(record.has_mobility for record in plotted)
    row_height = 4.45 if any_mobility else 4.1
    fig_height = max(4.2, row_height * len(plotted))
    fig = plt.figure(figsize=(24, fig_height))
    outer = fig.add_gridspec(len(plotted), 3, width_ratios=[1.25, 1.15, 1.7], hspace=0.24 if any_mobility else 0.34, wspace=0.18)
    for idx, record in enumerate(plotted):
        if record.has_mobility:
            left = outer[idx, 0].subgridspec(2, 2, hspace=0.52, wspace=0.04, height_ratios=[1.08, 0.92], width_ratios=[1.0, 0.035])
            left_top_ax = fig.add_subplot(left[0, 0])
            left_top_cax = fig.add_subplot(left[0, 1])
            left_bottom_ax = fig.add_subplot(left[1, 0], sharex=left_top_ax)
            left_bottom_cax = fig.add_subplot(left[1, 1])
            middle = outer[idx, 1].subgridspec(2, 2, hspace=0.52, wspace=0.04, height_ratios=[1.0, 1.0], width_ratios=[1.0, 0.035])
            middle_top_ax = fig.add_subplot(middle[0, 0])
            middle_top_cax = fig.add_subplot(middle[0, 1])
            middle_bottom_ax = fig.add_subplot(middle[1, 0], sharex=middle_top_ax)
            middle_bottom_cax = fig.add_subplot(middle[1, 1])
            right_ax = fig.add_subplot(outer[idx, 2])
            parent_x_values = np.concatenate((record.parent_peak_mz, record.parent_mobility_mz))
            parent_xlim = _mz_xlim_values(parent_x_values)
            zoom_bounds = _candidate_zoom_bounds(record)
            zoom_x_values = np.concatenate((record.parent_peak_mz, record.parent_mobility_mz, record.candidate_mz))
            zoom_xlim = _mz_xlim_values(zoom_x_values, zoom_bounds[0], zoom_bounds[1])
            _plot_parent_mobility(left_top_ax, record, fig, cax=left_top_cax)
            _plot_parent(left_bottom_ax, record, args.mz_tolerance_ppm, mz_xlim=parent_xlim)
            _hide_spacer_axis(left_bottom_cax)
            _apply_shared_x(left_top_ax, left_bottom_ax, parent_xlim)
            _plot_parent_mobility_zoom(middle_top_ax, record, fig, cax=middle_top_cax)
            _plot_parent_zoom(middle_bottom_ax, record, args.mz_tolerance_ppm, mz_xlim=zoom_xlim)
            _hide_spacer_axis(middle_bottom_cax)
            _apply_shared_x(middle_top_ax, middle_bottom_ax, zoom_xlim)
            _plot_msn(right_ax, record, args.mz_min, args.mz_max)
        else:
            left_ax = fig.add_subplot(outer[idx, 0])
            middle_ax = fig.add_subplot(outer[idx, 1])
            right_ax = fig.add_subplot(outer[idx, 2])
            _plot_parent(left_ax, record, args.mz_tolerance_ppm)
            _plot_parent_zoom(middle_ax, record, args.mz_tolerance_ppm)
            _plot_msn(right_ax, record, args.mz_min, args.mz_max)
    fig.suptitle(f"Raxport MSn Spectra: {input_path.name} (selected={len(records)}, plotted={len(plotted)}, rank_by={args.rank_by})", fontsize=13, y=1.0 - _figure_fraction_for_inches(0.14, fig_height))
    top = 1.0 - _figure_fraction_for_inches(0.48, fig_height)
    bottom = _figure_fraction_for_inches(0.36, fig_height)
    fig.subplots_adjust(left=0.04, right=0.992, top=top, bottom=bottom)
    fig.savefig(output_path, dpi=300, bbox_inches="tight")
    if args.show:
        plt.show()
    else:
        plt.close(fig)
    return len(plotted)


def _join_formatted(values):
    return ",".join(_format_value(value) for value in values)


def _join_ints(values):
    return ",".join(str(int(value)) for value in values)


def _deduplicated_precursor_rows(record, tolerance_ppm):
    grouped = {}
    parent_peak_indices = _matched_parent_peak_indices(record, tolerance_ppm)
    for candidate_index, candidate_mz in enumerate(record.candidate_mz):
        parent_peak_index = parent_peak_indices[candidate_index]
        key = ("parent", int(parent_peak_index)) if parent_peak_index >= 0 else ("candidate", round(float(candidate_mz), 6))
        group = grouped.setdefault(key, {"candidate_mz": [], "candidate_charge": [], "candidate_intensity": [], "candidate_one_over_k0": [], "parent_peak_index": parent_peak_index})
        group["candidate_mz"].append(float(candidate_mz))
        group["candidate_charge"].append(int(record.candidate_charge[candidate_index]))
        if candidate_index < record.candidate_intensity.size:
            group["candidate_intensity"].append(float(record.candidate_intensity[candidate_index]))
        if candidate_index < record.candidate_one_over_k0.size:
            group["candidate_one_over_k0"].append(float(record.candidate_one_over_k0[candidate_index]))
        if group["parent_peak_index"] < 0 and parent_peak_index >= 0:
            group["parent_peak_index"] = parent_peak_index
    rows = []
    for group in grouped.values():
        candidate_mz_values = group["candidate_mz"]
        candidate_charges = sorted(set(group["candidate_charge"]))
        parent_peak_index = group["parent_peak_index"]
        if parent_peak_index >= 0:
            parent_peak_mz = float(record.parent_peak_mz[parent_peak_index])
            parent_peak_intensity = float(record.parent_peak_intensity[parent_peak_index])
            parent_peak_charge = int(record.parent_peak_charge[parent_peak_index])
            delta_mz_values = [parent_peak_mz - candidate_mz for candidate_mz in candidate_mz_values]
            delta_ppm_values = [_delta_ppm(parent_peak_mz, candidate_mz) for candidate_mz in candidate_mz_values]
            row_mz = parent_peak_mz
            row_intensity = parent_peak_intensity
        else:
            parent_peak_mz = parent_peak_intensity = parent_peak_charge = None
            delta_mz_values = []
            delta_ppm_values = []
            row_mz = candidate_mz_values[0] if candidate_mz_values else None
            row_intensity = None
        rows.append({
            "mz": row_mz,
            "intensity": row_intensity,
            "charge": parent_peak_charge if parent_peak_charge not in (None, 0) else "",
            "candidate_mz": _join_formatted(candidate_mz_values),
            "candidate_charge": _join_ints(candidate_charges),
            "candidate_intensity": _join_formatted(group["candidate_intensity"]),
            "candidate_one_over_k0": _join_formatted(group["candidate_one_over_k0"]),
            "parent_peak_mz": parent_peak_mz,
            "parent_peak_intensity": parent_peak_intensity,
            "parent_peak_charge": parent_peak_charge if parent_peak_charge is not None else "",
            "parent_peak_delta_mz": _join_formatted(delta_mz_values),
            "parent_peak_delta_ppm": _join_formatted(delta_ppm_values),
            "guessed_charge": _join_ints(candidate_charges) if parent_peak_charge == 0 else "",
        })
    return rows


def _write_selected_tsv(records, output_path, tolerance_ppm):
    fieldnames = [
        "record_rank", "source_file", "scan_number", "ms_order", "retention_time", "tic", "parent_scan_number",
        "precursor_mass", "isolation_width", "one_over_k0_begin", "one_over_k0_end", "charge_state", "collision_energy",
        "row_kind", "row_index", "mz", "intensity", "charge", "candidate_mz", "candidate_charge", "candidate_intensity", "candidate_one_over_k0",
        "parent_peak_mz", "parent_peak_intensity", "parent_peak_charge", "parent_peak_delta_mz", "parent_peak_delta_ppm", "guessed_charge", "match_tolerance_ppm",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with output_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for record_rank, record in enumerate(records, start=1):
            base = {
                "record_rank": record_rank, "source_file": record.source_file, "scan_number": record.scan_number, "ms_order": record.ms_order,
                "retention_time": _format_value(record.retention_time), "tic": _format_value(record.tic), "parent_scan_number": record.parent_scan_number,
                "precursor_mass": _format_value(record.precursor_mass), "isolation_width": _format_value(record.isolation_width),
                "one_over_k0_begin": _format_value(record.one_over_k0_begin), "one_over_k0_end": _format_value(record.one_over_k0_end),
                "charge_state": record.charge_state or "", "collision_energy": _format_value(record.collision_energy),
            }
            for row_index, precursor_row in enumerate(_deduplicated_precursor_rows(record, tolerance_ppm)):
                row = dict(base)
                row.update({
                    "row_kind": "precursor_candidate", "row_index": row_index, "mz": _format_value(precursor_row["mz"]), "intensity": _format_value(precursor_row["intensity"]),
                    "charge": precursor_row["charge"], "candidate_mz": precursor_row["candidate_mz"], "candidate_charge": precursor_row["candidate_charge"],
                    "candidate_intensity": precursor_row["candidate_intensity"], "candidate_one_over_k0": precursor_row["candidate_one_over_k0"],
                    "parent_peak_mz": _format_value(precursor_row["parent_peak_mz"]), "parent_peak_intensity": _format_value(precursor_row["parent_peak_intensity"]),
                    "parent_peak_charge": precursor_row["parent_peak_charge"], "parent_peak_delta_mz": precursor_row["parent_peak_delta_mz"],
                    "parent_peak_delta_ppm": precursor_row["parent_peak_delta_ppm"], "guessed_charge": precursor_row["guessed_charge"], "match_tolerance_ppm": _format_value(tolerance_ppm),
                })
                writer.writerow(row)
                row_count += 1
            for row_index, mz in enumerate(record.peak_mz):
                row = dict(base)
                row.update({"row_kind": "spectrum_peak", "row_index": row_index, "mz": _format_value(float(mz)), "intensity": _format_value(float(record.peak_intensity[row_index])), "charge": int(record.peak_charge[row_index]), "match_tolerance_ppm": _format_value(tolerance_ppm)})
                writer.writerow(row)
                row_count += 1
    return row_count


def _print_file_summary(path):
    with h5py.File(path, "r") as handle:
        validate_hdf5_file(path)
        attrs = {key: _decode_hdf5_scalar_string(value) for key, value in handle.attrs.items()}
        has_mobility = _has_mobility(handle)
        ms_order = handle["scans/ms_order"][:].astype(int)
        reaction_count = handle["scans/reaction_count"][:].astype(int)
        msn_mask = ms_order > 1
        reaction_rows = handle["scans/reaction_start"][:].astype(np.int64)[msn_mask & (reaction_count > 0)]
        precursor_mass = handle["reactions/precursor_mass"][reaction_rows] if reaction_rows.size else np.asarray([])
        charge_state = handle["reactions/charge_state"][reaction_rows] if reaction_rows.size else np.asarray([])
        candidate_count = handle["reactions/candidate_count"][reaction_rows] if reaction_rows.size else np.asarray([])
        print(f"file={path}")
        print(f"attrs={attrs}")
        print(f"mobility_supported={has_mobility}")
        print(f"total_scans={len(ms_order)}")
        print(f"ms_order_counts={dict(sorted(Counter(map(int, ms_order)).items()))}")
        print(f"msn_scan_count={int(np.count_nonzero(msn_mask))}")
        print(f"msn_with_reaction_count={len(reaction_rows)}")
        print(_stats_text("precursor_mass", precursor_mass))
        print(f"charge_state_counts={dict(sorted(Counter(map(int, charge_state)).items()))}")
        print(_stats_text("candidate_count", candidate_count))
        if has_mobility and reaction_rows.size:
            print(_stats_text("one_over_k0_begin", handle["reactions/one_over_k0_begin"][reaction_rows]))
            print(_stats_text("one_over_k0_end", handle["reactions/one_over_k0_end"][reaction_rows]))
            print(_stats_text("candidate_one_over_k0", handle["precursor_candidates/one_over_k0"][:]))


def main():
    args = parse_args()
    input_path = Path(args.input).resolve()
    output_path = Path(args.output).resolve()
    tsv_output_path = Path(args.tsv_output).resolve() if args.tsv_output else output_path.with_suffix(".tsv")
    if args.top_k <= 0:
        raise SystemExit("--top-k must be positive.")
    if args.sample_size < 0:
        raise SystemExit("--sample-size must be non-negative.")
    if args.ms_order < 0:
        raise SystemExit("--ms-order must be non-negative.")
    if args.mz_tolerance_ppm < 0:
        raise SystemExit("--mz-tolerance-ppm must be non-negative.")
    if args.mz_min is not None and args.mz_max is not None and args.mz_min >= args.mz_max:
        raise SystemExit("--mz-min must be smaller than --mz-max.")
    if not input_path.exists():
        raise SystemExit(f"Input path not found: {input_path}")
    input_hdf5_paths = _input_hdf5_paths(input_path)
    for hdf5_path in input_hdf5_paths:
        _print_file_summary(hdf5_path)
    records = load_records(input_path, args.ms_order)
    if not records:
        raise SystemExit(f"No MSn scans found in {input_path}")
    selected = _select_records(records, args)
    if not selected:
        raise SystemExit("No scans matched the requested filters.")
    hydrate_records(selected)
    plotted_count = _plot_records(selected, input_path, output_path, args)
    tsv_row_count = _write_selected_tsv(selected, tsv_output_path, args.mz_tolerance_ppm)
    print(f"input_path={input_path}")
    print(f"input_hdf5_files={len(input_hdf5_paths)}")
    print(f"valid_msn_records={len(records)}")
    print(f"selected_records={len(selected)}")
    print(f"plotted_records={plotted_count}")
    print(f"tsv_rows={tsv_row_count}")
    print(f"precursor_match_tolerance_ppm={_format_value(args.mz_tolerance_ppm)}")
    for record in selected[:plotted_count]:
        k0 = _top_candidate_one_over_k0(record)
        mobility_text = f"\tprecursor_1_over_k0={_format_value(k0)}" if k0 is not None else ""
        print(f"plotted_scan={record.scan_number}\tMS{record.ms_order}\tRT={record.retention_time:.4f}\tTIC={_format_value(record.tic)}\tparent={record.parent_scan_number}\tprecursor={_format_value(record.precursor_mass)}\tcharge={record.charge_state or 'NA'}\tcandidates={record.candidate_mz.size}\tpeaks={record.peak_mz.size}\tmobility={record.has_mobility}{mobility_text}")
    print(f"output_plot={output_path}")
    print(f"output_tsv={tsv_output_path}")


if __name__ == "__main__":
    main()
