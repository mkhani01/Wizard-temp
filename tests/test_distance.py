#!/usr/bin/env python3
"""
Test distance calculation (OSRM) with a small example.
Requires caregivers.json and patient.json in project root or tests/data/ (optional).
Skips if files missing so pre-run checks still pass.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from distance_migration.osrm import load_json_data, get_distance_matrix


def test_small_example():
    """Test with a small subset of data. Returns True on success, False on failure, None if skipped."""
    for base in (PROJECT_ROOT, PROJECT_ROOT / "tests" / "data"):
        cpath = base / "caregivers.json"
        ppath = base / "patient.json"
        if cpath.exists() and ppath.exists():
            break
    else:
        print("  (Skipped: caregivers.json / patient.json not found)")
        return None

    caregivers_data = load_json_data(str(cpath))
    patients_data = load_json_data(str(ppath))
    caregiver_sample = {k: caregivers_data[k] for k in list(caregivers_data.keys())[:3]}
    patient_sample = {k: patients_data[k] for k in list(patients_data.keys())[:3]}

    caregiver_locs = {}
    for crid, caregiver in caregiver_sample.items():
        cid = caregiver["cid"]
        if cid not in caregiver_locs:
            caregiver_locs[cid] = {
                "longitude": caregiver["longitude"],
                "latitude": caregiver["latitude"],
            }
    patient_locs = {}
    for prid, patient in patient_sample.items():
        pid = patient["pid"]
        if pid not in patient_locs:
            patient_locs[pid] = {
                "longitude": patient["longitude"],
                "latitude": patient["latitude"],
            }

    cid_to_cid = get_distance_matrix(
        entities_info1=caregiver_locs,
        entities_info2=caregiver_locs,
        travel_method="driving-car",
        step_size=10,
    )
    for cid in caregiver_locs:
        key = (cid, cid)
        if key not in cid_to_cid["distance"]:
            print(f"  ✗ Missing same cid pair {cid}")
            return False
        if cid_to_cid["distance"][key] != 0:
            print(f"  ✗ Same cid {cid} distance should be 0")
            return False
    print("  ✓ Distance matrix test passed")
    return True


def test_pipeline_callback_does_not_accumulate():
    """Verify on_block_complete pipeline mode returns empty matrix."""
    from unittest.mock import patch

    locs_a = {1: {"longitude": -6.2, "latitude": 53.3}, 2: {"longitude": -6.3, "latitude": 53.4}}
    locs_b = {10: {"longitude": -6.25, "latitude": 53.35}}
    blocks = []

    def on_block(m):
        blocks.append(m)

    fake_block = {"distance": {(1, 10): 1.5}, "duration": {(1, 10): 5}}
    with patch("distance_migration.osrm.get_cross_distance_matrix", return_value=fake_block):
        result = get_distance_matrix(
            entities_info1=locs_a,
            entities_info2=locs_b,
            travel_method="driving-car",
            step_size=10,
            on_block_complete=on_block,
        )
    assert result.get("distance", {}) == {} or not result.get("distance")
    assert len(blocks) >= 1
    print("  ✓ Pipeline callback test passed")
    return True


if __name__ == "__main__":
    if not test_pipeline_callback_does_not_accumulate():
        sys.exit(1)
    result = test_small_example()
    if result is False:
        sys.exit(1)
    sys.exit(0)
