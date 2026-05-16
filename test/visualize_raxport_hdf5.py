import argparse
import csv
import math
import random
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

import h5py
import matplotlib.pyplot as plt
import numpy as np


MAX_PLOT_RECORDS = 8


@dataclass
class RaxportScanRecord:
    source_file: str
    source_index: int
    scan_number: int
    ms_order: int
    retention_time: float
    tic: float
    scan_filter: str
    activation: str
    parent_scan_number: int
    precursor_mass: float | None
    isolation_width: float | None
    charge_state: int | None
    collision_energy: float | None
    candidate_charge: np.ndarray
    candidate_mz: np.ndarray
    parent_peak_mz: np.ndarray
    parent_peak_intensity: np.ndarray
    parent_peak_charge: np.ndarray
    peak_mz: np.ndarray
    peak_intensity: np.ndarray
    peak_charge: np.ndarray


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Validate and visualize Raxport HDF5 output. The script checks the "
            "flat scan/peak/reaction offset schema, prints MSn precursor "
            "statistics, writes selected rows to TSV, and plots selected spectra."
        )
    )
    parser.add_argument(
        "--input",
        default="data/Pan_062822_X1iso5.h5",
        help="Raxport HDF5 file, or a directory containing .h5 files.",
    )
    parser.add_argument(
        "--output",
        default="test/raxport_hdf5_spectra_top.pdf",
        help="Output plot path.",
    )
    parser.add_argument(
        "--tsv-output",
        default="",
        help="Output TSV path. Defaults to the plot path with .tsv suffix.",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of scans to select; plots at most 8.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Number of candidate scans to sample before ranking. Use 0 to rank all scans.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed used when sampling scans.",
    )
    parser.add_argument(
        "--rank-by",
        choices=["tic", "peak-count", "candidate-count", "source-order", "random"],
        default="tic",
        help="How selected scans are ranked after sampling.",
    )
    parser.add_argument(
        "--ms-order",
        type=int,
        default=0,
        help="Only consider one MS order. Use 0 for all MSn scans where ms_order > 1.",
    )
    parser.add_argument(
        "--scan-number",
        action="append",
        type=int,
        default=[],
        help="Plot a specific scan number. Can be supplied multiple times; bypasses sampling/ranking.",
    )
    parser.add_argument(
        "--mz-min",
        type=float,
        default=None,
        help="Optional lower m/z bound for the spectrum panel.",
    )
    parser.add_argument(
        "--mz-max",
        type=float,
        default=None,
        help="Optional upper m/z bound for the spectrum panel.",
    )
    parser.add_argument(
        "--mz-tolerance-ppm",
        type=float,
        default=10.0,
        help="PPM tolerance used to match HDF5 precursor candidates back to parent MS1 peaks.",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display the figure after saving it.",
    )
    return parser.parse_args()


def _decode_hdf5_strings(values):
    decoded = []
    for value in values:
        decoded.append(_decode_hdf5_scalar_string(value))
    return decoded


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
        files = sorted(
            item
            for item in path.iterdir()
            if item.is_file() and item.suffix.lower() in {".h5", ".hdf5"}
        )
        if not files:
            raise ValueError(f"{path}: no .h5 or .hdf5 files found")
        return files
    if path.suffix.lower() not in {".h5", ".hdf5"}:
        raise ValueError(f"{path}: expected an HDF5 file or directory")
    return [path]


def _format_value(value):
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isfinite(value):
            return f"{value:.10g}"
        return ""
    return str(value)


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


def _validate_required_datasets(handle, path):
    required = [
        "scans/scan_number",
        "scans/ms_order",
        "scans/retention_time",
        "scans/tic",
        "scans/parent_scan_number",
        "scans/reaction_start",
        "scans/reaction_count",
        "scans/peak_start",
        "scans/peak_count",
        "peaks/mz",
        "peaks/intensity",
        "peaks/charge",
        "reactions/precursor_mass",
        "reactions/isolation_width",
        "reactions/charge_state",
        "reactions/collision_energy",
        "reactions/candidate_start",
        "reactions/candidate_count",
        "precursor_candidates/charge",
        "precursor_candidates/mz",
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
    scan_datasets = [
        "ms_order",
        "retention_time",
        "tic",
        "parent_scan_number",
        "reaction_start",
        "reaction_count",
        "peak_start",
        "peak_count",
    ]
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


def validate_hdf5_file(path):
    with h5py.File(path, "r") as handle:
        _validate_required_datasets(handle, path)
        _validate_offsets(handle, path)


def load_hdf5_records(path, ms_order_filter=0):
    records = []
    with h5py.File(path, "r") as handle:
        _validate_required_datasets(handle, path)
        _validate_offsets(handle, path)

        scans = handle["scans"]
        peaks = handle["peaks"]
        reactions = handle["reactions"]
        candidates = handle["precursor_candidates"]

        scan_number = scans["scan_number"][:].astype(int)
        ms_order = scans["ms_order"][:].astype(int)
        retention_time = scans["retention_time"][:].astype(float)
        tic = scans["tic"][:].astype(float)
        scan_filter = _read_scan_strings(scans, handle, "scan_filter", "scan_filter_id")
        activation = _read_scan_strings(scans, handle, "activation", "activation_id")
        parent_scan_number = scans["parent_scan_number"][:].astype(int)
        reaction_start = scans["reaction_start"][:].astype(np.int64)
        reaction_count = scans["reaction_count"][:].astype(np.int64)
        peak_start = scans["peak_start"][:].astype(np.int64)
        peak_count = scans["peak_count"][:].astype(np.int64)
        scan_index_by_number = {int(number): index for index, number in enumerate(scan_number)}

        peak_mz = peaks["mz"]
        peak_intensity = peaks["intensity"]
        peak_charge = peaks["charge"]

        precursor_mass = reactions["precursor_mass"]
        isolation_width = reactions["isolation_width"]
        charge_state = reactions["charge_state"]
        collision_energy = reactions["collision_energy"]
        candidate_start = reactions["candidate_start"][:].astype(np.int64)
        candidate_count = reactions["candidate_count"][:].astype(np.int64)

        candidate_charge = candidates["charge"]
        candidate_mz = candidates["mz"]

        for index in range(len(scan_number)):
            if ms_order[index] <= 1:
                continue
            if ms_order_filter > 0 and ms_order[index] != ms_order_filter:
                continue

            p_start = int(peak_start[index])
            p_stop = p_start + int(peak_count[index])
            parent_index = scan_index_by_number.get(int(parent_scan_number[index]))
            if parent_index is None:
                parent_p_start = 0
                parent_p_stop = 0
            else:
                parent_p_start = int(peak_start[parent_index])
                parent_p_stop = parent_p_start + int(peak_count[parent_index])

            precursor = None
            width = None
            charge = None
            energy = None
            cand_charge_array = np.asarray([], dtype=int)
            cand_mz_array = np.asarray([], dtype=float)
            if reaction_count[index] > 0:
                r_index = int(reaction_start[index])
                precursor = float(precursor_mass[r_index])
                width = float(isolation_width[r_index])
                charge = int(charge_state[r_index])
                energy = float(collision_energy[r_index])
                c_start = int(candidate_start[r_index])
                c_stop = c_start + int(candidate_count[r_index])
                cand_charge_array = np.asarray(candidate_charge[c_start:c_stop], dtype=int)
                cand_mz_array = np.asarray(candidate_mz[c_start:c_stop], dtype=float)

            records.append(
                RaxportScanRecord(
                    source_file=path.name,
                    source_index=index,
                    scan_number=int(scan_number[index]),
                    ms_order=int(ms_order[index]),
                    retention_time=float(retention_time[index]),
                    tic=float(tic[index]),
                    scan_filter=scan_filter[index],
                    activation=activation[index],
                    parent_scan_number=int(parent_scan_number[index]),
                    precursor_mass=precursor,
                    isolation_width=width,
                    charge_state=charge,
                    collision_energy=energy,
                    candidate_charge=cand_charge_array,
                    candidate_mz=cand_mz_array,
                    parent_peak_mz=np.asarray(peak_mz[parent_p_start:parent_p_stop], dtype=float),
                    parent_peak_intensity=np.asarray(peak_intensity[parent_p_start:parent_p_stop], dtype=float),
                    parent_peak_charge=np.asarray(peak_charge[parent_p_start:parent_p_stop], dtype=int),
                    peak_mz=np.asarray(peak_mz[p_start:p_stop], dtype=float),
                    peak_intensity=np.asarray(peak_intensity[p_start:p_stop], dtype=float),
                    peak_charge=np.asarray(peak_charge[p_start:p_stop], dtype=int),
                )
            )
    return records


def load_records(path, ms_order_filter=0):
    records = []
    for hdf5_path in _input_hdf5_paths(path):
        records.extend(load_hdf5_records(hdf5_path, ms_order_filter))
    return records


def _sample_records(records, sample_size, seed):
    if sample_size == 0 or len(records) <= sample_size:
        return list(records)
    rng = random.Random(seed)
    return rng.sample(records, sample_size)


def _select_records(records, args):
    if args.scan_number:
        by_scan = {record.scan_number: record for record in records}
        missing = [str(scan) for scan in args.scan_number if scan not in by_scan]
        if missing:
            raise SystemExit(f"Scan number not found among selected MSn scans: {', '.join(missing)}")
        return [by_scan[scan] for scan in args.scan_number]

    sampled = _sample_records(records, args.sample_size, args.seed)
    if args.rank_by == "random":
        rng = random.Random(args.seed)
        rng.shuffle(sampled)
        return sampled[: args.top_k]
    if args.rank_by == "source-order":
        ranked = sorted(sampled, key=lambda record: record.source_index)
    elif args.rank_by == "peak-count":
        ranked = sorted(sampled, key=lambda record: (record.peak_mz.size, record.tic), reverse=True)
    elif args.rank_by == "candidate-count":
        ranked = sorted(sampled, key=lambda record: (record.candidate_mz.size, record.tic), reverse=True)
    else:
        ranked = sorted(sampled, key=lambda record: (record.tic, record.peak_mz.size), reverse=True)
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


def _plot_peak_series(ax, mz, intensity, color, label, linewidth=0.4, alpha=0.65):
    if mz.size == 0:
        return
    ax.vlines(mz, 0.0, intensity, color=color, alpha=alpha, linewidth=linewidth, label=label)


def _dedup_legend(ax):
    handles, labels = ax.get_legend_handles_labels()
    dedup = {}
    for handle, label in zip(handles, labels):
        dedup.setdefault(label, handle)
    if dedup:
        ax.legend(dedup.values(), dedup.keys(), loc="upper right", fontsize=8)


def _normalize_intensity(intensity):
    if intensity.size and float(np.max(intensity)) > 0.0:
        return intensity / float(np.max(intensity))
    return intensity


def _mz_mask(mz, mz_min=None, mz_max=None):
    if mz_min is None and mz_max is None:
        return np.ones(mz.shape[0], dtype=bool)
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
    indices = []
    for candidate_mz in record.candidate_mz:
        indices.append(_nearest_peak_index_within_tolerance(record.parent_peak_mz, float(candidate_mz), tolerance_ppm))
    return indices


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
    return (
        f"top precursor m/z={top_mz} z={top_charge}\n"
        f"isolation center={_format_value(record.precursor_mass)} | "
        f"precursor count={len(unique_indices)}"
    )


def _draw_precursor_markers(ax, record, tolerance_ppm, plotted_mz=None, plotted_intensity=None, max_arrows=None):
    if record.precursor_mass is not None:
        ax.axvline(
            record.precursor_mass,
            color="tab:red",
            linestyle="--",
            linewidth=1.0,
            alpha=0.85,
            label="Isolation center",
        )
        if record.isolation_width is not None:
            half_width = record.isolation_width / 2.0
            ax.axvspan(
                record.precursor_mass - half_width,
                record.precursor_mass + half_width,
                color="tab:red",
                alpha=0.08,
                label="Isolation window",
            )

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
        if parent_peak_index < 0:
            continue
        if parent_peak_index in used_parent_peak_indices:
            continue
        used_parent_peak_indices.add(parent_peak_index)
        parent_peak_mz = float(record.parent_peak_mz[parent_peak_index])
        plotted_peak_index = _nearest_peak_index_within_tolerance(mz_values, parent_peak_mz, tolerance_ppm)
        if plotted_peak_index < 0:
            continue
        peak_mz = float(mz_values[plotted_peak_index])
        peak_y = float(intensity_values[plotted_peak_index]) if intensity_values.size else 0.0
        ax.vlines(
            [peak_mz],
            0.0,
            [peak_y],
            color="tab:red",
            linewidth=1.0,
            alpha=0.95,
            label="Precursor peak" if arrow_count == 0 else None,
        )
        arrow_tip = min(1.03, max(0.04, peak_y + 0.025))
        arrow_tail = min(1.1, max(0.16, peak_y + 0.14))
        ax.annotate(
            "",
            xy=(peak_mz, arrow_tip),
            xytext=(peak_mz, arrow_tail),
            arrowprops={
                "arrowstyle": "-|>",
                "color": "tab:orange",
                "linewidth": 1.0,
                "alpha": 0.9,
                "shrinkA": 0,
                "shrinkB": 0,
            },
        )
        arrow_count += 1
    if arrow_count:
        label = "Top precursor" if max_arrows == 1 else "Top precursor candidates"
        ax.plot([], [], color="tab:orange", linewidth=1.2, label=label)


def _draw_unselected_parent_peaks(ax, record, tolerance_ppm, plotted_mz, plotted_intensity, max_arrows=None):
    if plotted_mz.size == 0:
        return
    matched_parent_indices = set(_matched_parent_peak_indices(record, tolerance_ppm))
    matched_parent_indices.discard(-1)
    if max_arrows is not None:
        parent_indices = _matched_parent_peak_indices(record, tolerance_ppm)
        limited = set(parent_indices[index] for index in _unique_candidate_indices(record)[:max_arrows])
        limited.discard(-1)
        matched_parent_indices = limited

    unselected_mz = []
    unselected_intensity = []
    for mz, intensity in zip(plotted_mz, plotted_intensity):
        parent_index = _nearest_peak_index_within_tolerance(record.parent_peak_mz, float(mz), tolerance_ppm)
        if parent_index not in matched_parent_indices:
            unselected_mz.append(float(mz))
            unselected_intensity.append(float(intensity))

    if unselected_mz:
        ax.vlines(
            np.asarray(unselected_mz),
            0.0,
            np.asarray(unselected_intensity),
            color="tab:green",
            linewidth=0.45,
            alpha=0.9,
            label="Unselected parent peaks",
        )


def _plot_parent(ax, record, tolerance_ppm):
    intensity = _normalize_intensity(record.parent_peak_intensity)
    _draw_precursor_markers(ax, record, tolerance_ppm, record.parent_peak_mz, intensity, max_arrows=1)
    _draw_unselected_parent_peaks(ax, record, tolerance_ppm, record.parent_peak_mz, intensity, max_arrows=1)
    _set_mz_xlim(ax, record.parent_peak_mz)
    ax.set_ylim(0.0, 1.12)
    ax.set_xlabel("m/z")
    ax.set_ylabel("Relative intensity")
    ax.set_title(f"Parent MS1 scan {record.parent_scan_number}\n{_top_precursor_text(record)}", fontsize=9)
    _dedup_legend(ax)


def _plot_parent_zoom(ax, record, tolerance_ppm):
    mz_min, mz_max = _candidate_zoom_bounds(record)
    mask = _mz_mask(record.parent_peak_mz, mz_min, mz_max)
    mz = record.parent_peak_mz[mask]
    intensity = _normalize_intensity(record.parent_peak_intensity[mask])

    _draw_precursor_markers(ax, record, tolerance_ppm, mz, intensity)
    _draw_unselected_parent_peaks(ax, record, tolerance_ppm, mz, intensity)
    _set_mz_xlim(ax, mz if mz.size else record.candidate_mz, mz_min, mz_max)
    ax.set_ylim(0.0, 1.12)
    ax.set_xlabel("m/z")
    ax.set_ylabel("Relative intensity")
    ax.set_title("Parent zoom: candidate precursors", fontsize=9)
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
    title = (
        f"{record.source_file} | scan={record.scan_number} | MS{record.ms_order} | "
        f"RT={record.retention_time:.4f} | parent={record.parent_scan_number} | "
        f"precursor={_format_value(record.precursor_mass)} | z={record.charge_state or 'NA'} | "
        f"candidates={record.candidate_mz.size} | peaks={record.peak_mz.size}"
    )
    ax.set_title(title, fontsize=9)
    _dedup_legend(ax)


def _plot_records(records, input_path, output_path, args):
    plotted = records[: min(MAX_PLOT_RECORDS, len(records))]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(
        len(plotted),
        3,
        figsize=(24, 4.4 * len(plotted)),
        squeeze=False,
        gridspec_kw={"width_ratios": [1.2, 1.0, 1.7]},
    )
    fig.subplots_adjust(left=0.045, right=0.99, top=0.92, bottom=0.06, hspace=0.45, wspace=0.18)
    for idx, record in enumerate(plotted):
        _plot_parent(axes[idx][0], record, args.mz_tolerance_ppm)
        _plot_parent_zoom(axes[idx][1], record, args.mz_tolerance_ppm)
        _plot_msn(axes[idx][2], record, args.mz_min, args.mz_max)
    fig.suptitle(
        f"Raxport MSn Spectra from {input_path.name} "
        f"(selected={len(records)}, plotted={len(plotted)}, rank_by={args.rank_by})",
        fontsize=15,
    )
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
        if parent_peak_index >= 0:
            key = ("parent", int(parent_peak_index))
        else:
            key = ("candidate", round(float(candidate_mz), 6))

        group = grouped.setdefault(
            key,
            {
                "candidate_mz": [],
                "candidate_charge": [],
                "parent_peak_index": parent_peak_index,
            },
        )
        group["candidate_mz"].append(float(candidate_mz))
        group["candidate_charge"].append(int(record.candidate_charge[candidate_index]))
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
            parent_peak_mz = None
            parent_peak_intensity = None
            parent_peak_charge = None
            delta_mz_values = []
            delta_ppm_values = []
            row_mz = candidate_mz_values[0] if candidate_mz_values else None
            row_intensity = None

        real_charge = parent_peak_charge if parent_peak_charge not in (None, 0) else ""
        guessed_charge = _join_ints(candidate_charges) if parent_peak_charge == 0 else ""
        rows.append(
            {
                "mz": row_mz,
                "intensity": row_intensity,
                "charge": real_charge,
                "candidate_mz": _join_formatted(candidate_mz_values),
                "candidate_charge": _join_ints(candidate_charges),
                "parent_peak_mz": parent_peak_mz,
                "parent_peak_intensity": parent_peak_intensity,
                "parent_peak_charge": parent_peak_charge if parent_peak_charge is not None else "",
                "parent_peak_delta_mz": _join_formatted(delta_mz_values),
                "parent_peak_delta_ppm": _join_formatted(delta_ppm_values),
                "guessed_charge": guessed_charge,
            }
        )

    return rows


def _write_selected_tsv(records, output_path, tolerance_ppm):
    fieldnames = [
        "record_rank",
        "source_file",
        "scan_number",
        "ms_order",
        "retention_time",
        "tic",
        "parent_scan_number",
        "precursor_mass",
        "isolation_width",
        "charge_state",
        "collision_energy",
        "row_kind",
        "row_index",
        "mz",
        "intensity",
        "charge",
        "candidate_mz",
        "candidate_charge",
        "parent_peak_mz",
        "parent_peak_intensity",
        "parent_peak_charge",
        "parent_peak_delta_mz",
        "parent_peak_delta_ppm",
        "guessed_charge",
        "match_tolerance_ppm",
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with output_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for record_rank, record in enumerate(records, start=1):
            base = {
                "record_rank": record_rank,
                "source_file": record.source_file,
                "scan_number": record.scan_number,
                "ms_order": record.ms_order,
                "retention_time": _format_value(record.retention_time),
                "tic": _format_value(record.tic),
                "parent_scan_number": record.parent_scan_number,
                "precursor_mass": _format_value(record.precursor_mass),
                "isolation_width": _format_value(record.isolation_width),
                "charge_state": record.charge_state or "",
                "collision_energy": _format_value(record.collision_energy),
            }
            for row_index, precursor_row in enumerate(_deduplicated_precursor_rows(record, tolerance_ppm)):
                row = dict(base)
                row.update(
                    {
                        "row_kind": "precursor_candidate",
                        "row_index": row_index,
                        "mz": _format_value(precursor_row["mz"]),
                        "intensity": _format_value(precursor_row["intensity"]),
                        "charge": precursor_row["charge"],
                        "candidate_mz": precursor_row["candidate_mz"],
                        "candidate_charge": precursor_row["candidate_charge"],
                        "parent_peak_mz": _format_value(precursor_row["parent_peak_mz"]),
                        "parent_peak_intensity": _format_value(precursor_row["parent_peak_intensity"]),
                        "parent_peak_charge": precursor_row["parent_peak_charge"],
                        "parent_peak_delta_mz": precursor_row["parent_peak_delta_mz"],
                        "parent_peak_delta_ppm": precursor_row["parent_peak_delta_ppm"],
                        "guessed_charge": precursor_row["guessed_charge"],
                        "match_tolerance_ppm": _format_value(tolerance_ppm),
                    }
                )
                writer.writerow(row)
                row_count += 1
            for row_index, mz in enumerate(record.peak_mz):
                row = dict(base)
                row.update(
                    {
                        "row_kind": "spectrum_peak",
                        "row_index": row_index,
                        "mz": _format_value(float(mz)),
                        "intensity": _format_value(float(record.peak_intensity[row_index])),
                        "charge": int(record.peak_charge[row_index]),
                        "match_tolerance_ppm": _format_value(tolerance_ppm),
                    }
                )
                writer.writerow(row)
                row_count += 1
    return row_count


def _print_file_summary(path):
    with h5py.File(path, "r") as handle:
        validate_hdf5_file(path)
        attrs = {key: _decode_hdf5_scalar_string(value) for key, value in handle.attrs.items()}
        ms_order = handle["scans/ms_order"][:].astype(int)
        reaction_count = handle["scans/reaction_count"][:].astype(int)
        msn_mask = ms_order > 1
        reaction_rows = handle["scans/reaction_start"][:].astype(np.int64)[msn_mask & (reaction_count > 0)]
        precursor_mass = handle["reactions/precursor_mass"][reaction_rows] if reaction_rows.size else np.asarray([])
        charge_state = handle["reactions/charge_state"][reaction_rows] if reaction_rows.size else np.asarray([])
        candidate_count = handle["reactions/candidate_count"][reaction_rows] if reaction_rows.size else np.asarray([])

        print(f"file={path}")
        print(f"attrs={attrs}")
        print(f"total_scans={len(ms_order)}")
        print(f"ms_order_counts={dict(sorted(Counter(map(int, ms_order)).items()))}")
        print(f"msn_scan_count={int(np.count_nonzero(msn_mask))}")
        print(f"msn_with_reaction_count={len(reaction_rows)}")
        print(_stats_text("precursor_mass", precursor_mass))
        print(f"charge_state_counts={dict(sorted(Counter(map(int, charge_state)).items()))}")
        print(_stats_text("candidate_count", candidate_count))


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
        print(
            f"plotted_scan={record.scan_number}\t"
            f"MS{record.ms_order}\t"
            f"RT={record.retention_time:.4f}\t"
            f"TIC={_format_value(record.tic)}\t"
            f"parent={record.parent_scan_number}\t"
            f"precursor={_format_value(record.precursor_mass)}\t"
            f"charge={record.charge_state or 'NA'}\t"
            f"candidates={record.candidate_mz.size}\t"
            f"peaks={record.peak_mz.size}"
        )
    print(f"output_plot={output_path}")
    print(f"output_tsv={tsv_output_path}")


if __name__ == "__main__":
    main()
