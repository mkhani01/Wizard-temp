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


if __name__ == "__main__":
    result = test_small_example()
    if result is None:
        sys.exit(0)  # skip
    sys.exit(0 if result else 1)
