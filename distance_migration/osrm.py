"""
OSRM table API client: distance and duration matrices for driving, cycling, walking.
"""

import os
import sys
from typing import List, Dict, Literal, Tuple, Optional, Callable, Set
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse
import json
import time
import requests

from geopy.distance import geodesic

# ----------------------
# OSRM Configuration
OSRM_CONFIG = {
    'driving-car': {
        'base_path': '/car/table/v1/driving',
        'profile_name': 'driving'
    },
    'cycling-regular': {
        'base_path': '/bicycle/table/v1/cycling',
        'profile_name': 'cycling'
    },
    'foot-walking': {
        'base_path': '/foot/table/v1/walking',
        'profile_name': 'walking'
    }
}

OSRM_BASE_URL = 'https://osrm.aossystem.com'
OSRM_REQUEST_TIMEOUT_SECONDS = 120
OSRM_MAX_RETRIES = 5
OSRM_BACKOFF_SECONDS = 2
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def call_osrm_table_api(
    locations: List[Tuple[float, float]],
    travel_method: Literal['driving-car', 'cycling-regular', 'foot-walking'],
    sources: List[int] = None,
    destinations: List[int] = None
) -> Dict:
    """
    Calls OSRM table API to get distance and duration matrix.
    """
    assert travel_method in OSRM_CONFIG, f"Unknown travel method: {travel_method}"

    config = OSRM_CONFIG[travel_method]
    base_path = config['base_path']

    # Format coordinates as lon,lat;lon,lat;...
    coordinates_str = ';'.join([f"{lon},{lat}" for lon, lat in locations])

    # Build URL
    url = f"{OSRM_BASE_URL}{base_path}/{coordinates_str}"

    # Add query parameters
    params = {'annotations': 'distance,duration'}
    if sources is not None:
        params['sources'] = ';'.join(map(str, sources))
    if destinations is not None:
        params['destinations'] = ';'.join(map(str, destinations))

    last_error = None
    for attempt in range(1, OSRM_MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=OSRM_REQUEST_TIMEOUT_SECONDS)
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise requests.exceptions.HTTPError(
                    f"Retryable OSRM status {response.status_code}",
                    response=response,
                )
            response.raise_for_status()
            data = response.json()

            if data.get('code') != 'Ok':
                raise ValueError(f"OSRM API error: {data.get('message', 'Unknown error')}")

            return {
                'distances': data.get('distances', []),
                'durations': data.get('durations', [])
            }
        except requests.exceptions.RequestException as e:
            last_error = e
            if attempt == OSRM_MAX_RETRIES:
                break
            delay = OSRM_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(
                f"OSRM request failed on attempt {attempt}/{OSRM_MAX_RETRIES}: {e}. "
                f"Retrying in {delay}s..."
            )
            time.sleep(delay)

    raise RuntimeError(f"Failed to call OSRM API after {OSRM_MAX_RETRIES} attempts: {str(last_error)}")


def get_cross_distance_matrix(
    ent1: Dict,
    ent2: Dict,
    travel_method: Literal['driving-car', 'cycling-regular', 'foot-walking'],
    is_self_matrix: bool = False
) -> Dict:
    """
    Computes distance matrix from ent1 → ent2.
    
    Args:
        ent1: Dictionary of entities {id: {longitude, latitude}} (Sources)
        ent2: Dictionary of entities {id: {longitude, latitude}} (Destinations)
        travel_method: Travel method to use
        is_self_matrix: True if ent1 and ent2 are the same set (handles diagonal zeroing).
    """
    assert travel_method in ('driving-car', 'cycling-regular', 'foot-walking')

    ent1_ids = list(ent1.keys())
    ent2_ids = list(ent2.keys())

    if not ent1_ids or not ent2_ids:
        return {"distance": {}, "duration": {}}

    locations = []
    src_idx = []
    dst_idx = []

    # ent1 → sources (using lon,lat format for OSRM)
    for i, eid in enumerate(ent1_ids):
        locations.append((ent1[eid]['longitude'], ent1[eid]['latitude']))
        src_idx.append(i)

    # ent2 → destinations
    offset = len(locations)
    for j, pid in enumerate(ent2_ids):
        locations.append((ent2[pid]['longitude'], ent2[pid]['latitude']))
        dst_idx.append(offset + j)

    matrix = call_osrm_table_api(
        locations=locations,
        travel_method=travel_method,
        sources=src_idx,
        destinations=dst_idx
    )

    distance = {}
    duration = {}

    for i, eid in enumerate(ent1_ids):
        for j, pid in enumerate(ent2_ids):
            # Handle Self Matrix Diagonal (User->User same ID)
            if is_self_matrix and eid == pid:
                distance[(eid, pid)] = 0.0
                duration[(eid, pid)] = 0
                continue

            dist = matrix['distances'][i][j]
            du = matrix['durations'][i][j]

            if dist is None or dist < 0:
                # Fallback for nulls/invalid using geodesic
                p1 = (ent1[eid]['latitude'], ent1[eid]['longitude'])
                p2 = (ent2[pid]['latitude'], ent2[pid]['longitude'])
                geo_dist = geodesic(p1, p2)
                if float(geo_dist.meters) < 500:
                    dist = 0
                    du = 0
                else:
                    raise ValueError(f"Distance is None or invalid for ent1[{eid}], ent2[{pid}]: {dist}")

            distance[(eid, pid)] = round(dist / 1000, 4)
            duration[(eid, pid)] = int(du / 60)

    return {
        "distance": distance,
        "duration": duration
    }


# Max concurrent OSRM block requests (avoid overwhelming server)
OSRM_MAX_WORKERS = 4


def _block_covers_required_pairs(
    blk1: Tuple[int, int],
    blk2: Tuple[int, int],
    entity_ids1: List,
    entity_ids2: List,
    num_entity1: int,
    num_entity2: int,
    required_pairs: Optional[Set[Tuple[int, int]]],
) -> bool:
    """True if this source×dest block could contain any required pair."""
    if not required_pairs:
        return True
    src_ids = entity_ids1[blk1[0]: min(blk1[1], num_entity1)]
    dst_ids = entity_ids2[blk2[0]: min(blk2[1], num_entity2)]
    for s in src_ids:
        for t in dst_ids:
            if (s, t) in required_pairs:
                return True
    return False


def get_distance_matrix(
    entities_info1: Dict,
    entities_info2: Dict,
    travel_method: Literal['driving-car', 'cycling-regular', 'foot-walking'],
    step_size: int = 50,
    on_block_complete: Optional[Callable[[Dict], None]] = None,
    required_pairs: Optional[Set[Tuple[int, int]]] = None,
) -> Dict:
    """
    Computes distance matrix between two sets of entities with batching.
    Block pairs are requested in parallel via ThreadPoolExecutor.

    If on_block_complete is provided, each successful block is passed to the callback
    immediately and is not accumulated in the returned matrix (pipeline mode).
    required_pairs optionally filters block results to (src_id, dst_id) in the set.
    """
    entity_ids1 = list(entities_info1.keys())
    num_entity1 = len(entities_info1)
    entity_ids2 = list(entities_info2.keys())
    num_entity2 = len(entities_info2)

    blocks1 = [(i, i + step_size) for i in range(0, num_entity1, step_size)]
    blocks2 = [(i, i + step_size) for i in range(0, num_entity2, step_size)]

    block_pairs = [
        (blk1, blk2)
        for blk1 in blocks1
        for blk2 in blocks2
        if _block_covers_required_pairs(
            blk1, blk2, entity_ids1, entity_ids2,
            num_entity1, num_entity2, required_pairs,
        )
    ]

    accumulate = on_block_complete is None
    distance_info = {'distance': {}, 'duration': {}} if accumulate else {}

    is_self_matrix = (entities_info1 is entities_info2)

    total_blocks = len(block_pairs)
    skipped_blocks = (len(blocks1) * len(blocks2)) - total_blocks
    if skipped_blocks:
        print(f"Skipping {skipped_blocks} block(s) with no missing required pairs.")
    print(f"Processing {num_entity1} x {num_entity2} matrix with {travel_method} ({total_blocks} blocks, {OSRM_MAX_WORKERS} workers)...")

    def _filter_block(blk_distance: Dict) -> Dict:
        if not required_pairs:
            return blk_distance
        filtered = {"distance": {}, "duration": {}}
        for key, dist_km in blk_distance.get("distance", {}).items():
            if key in required_pairs:
                filtered["distance"][key] = dist_km
                filtered["duration"][key] = blk_distance.get("duration", {}).get(key, 0)
        return filtered

    def run_block(blk1, blk2):
        src_entities = {
            entity_ids1[i]: entities_info1[entity_ids1[i]]
            for i in range(blk1[0], min(blk1[1], num_entity1))
        }
        dst_entities = {
            entity_ids2[j]: entities_info2[entity_ids2[j]]
            for j in range(blk2[0], min(blk2[1], num_entity2))
        }
        return get_cross_distance_matrix(
            ent1=src_entities,
            ent2=dst_entities,
            travel_method=travel_method,
            is_self_matrix=is_self_matrix
        )

    completed = 0
    errors = []
    if not block_pairs:
        return distance_info

    with ThreadPoolExecutor(max_workers=OSRM_MAX_WORKERS) as executor:
        futures = {
            executor.submit(run_block, blk1, blk2): (blk1, blk2)
            for blk1, blk2 in block_pairs
        }
        for future in as_completed(futures):
            blk1, blk2 = futures[future]
            try:
                blk_distance = future.result()
            except Exception as e:
                errors.append((blk1, blk2, str(e)))
                print(
                    f"  Block {blk1[0]}:{blk1[1]} x {blk2[0]}:{blk2[1]} failed after retries: {e}"
                )
                continue
            blk_distance = _filter_block(blk_distance)
            if on_block_complete:
                if blk_distance.get("distance"):
                    on_block_complete(blk_distance)
            elif accumulate:
                distance_info['distance'].update(blk_distance['distance'])
                distance_info['duration'].update(blk_distance['duration'])
            completed += 1
            if completed % 20 == 0 or completed == total_blocks:
                print(f"  Processing block {completed}/{total_blocks}...")

    if errors:
        distance_info['errors'] = errors
        print(
            f"Completed {completed}/{total_blocks} blocks with {len(errors)} failed block(s). "
            "Successful blocks will be inserted before retry."
        )

    return distance_info


def load_json_data(file_path: str) -> Dict:
    """Load JSON data from file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def validate_outputs(
    output_data: Dict,
    caregiver_locations: Dict,
    patient_locations: Dict
):
    """Validate that all required pairs are present in the outputs."""
    cids = list(caregiver_locations.keys())
    pids = list(patient_locations.keys())
    distance_dict = output_data['distance']

    missing_cid_cid = [f"{c1}_{c2}" for c1 in cids for c2 in cids if f"{c1}_{c2}" not in distance_dict]
    missing_pid_pid = [f"{p1}_{p2}" for p1 in pids for p2 in pids if f"{p1}_{p2}" not in distance_dict]
    missing_cid_pid = [f"{c}_{p}" for c in cids for p in pids if f"{c}_{p}" not in distance_dict]
    missing_pid_cid = [f"{p}_{c}" for p in pids for c in cids if f"{p}_{c}" not in distance_dict]

    if missing_cid_cid:
        print(f"  WARNING: Missing {len(missing_cid_cid)} cid_to_cid pairs (first 5): {missing_cid_cid[:5]}")
    if missing_pid_pid:
        print(f"  WARNING: Missing {len(missing_pid_pid)} pid_to_pid pairs (first 5): {missing_pid_pid[:5]}")
    if missing_cid_pid:
        print(f"  WARNING: Missing {len(missing_cid_pid)} cid_to_pid pairs (first 5): {missing_cid_pid[:5]}")
    if missing_pid_cid:
        print(f"  WARNING: Missing {len(missing_pid_cid)} pid_to_cid pairs (first 5): {missing_pid_cid[:5]}")

    total_expected = len(cids)**2 + len(pids)**2 + len(cids)*len(pids) + len(pids)*len(cids)
    print(f"  Total pairs: {len(distance_dict)} (expected: {total_expected})")


def process_all_pairs(
    caregivers_file: str,
    patients_file: str,
    output_dir: str = "output",
    step_size: int = 50
):
    """
    Process all pairs and generate distance matrices for all three travel methods.
    CLI entry: python -m distance_migration.osrm --caregivers ... --patients ...
    """
    os.makedirs(output_dir, exist_ok=True)

    print("Loading caregivers and patients data...")
    caregivers_data = load_json_data(caregivers_file)
    patients_data = load_json_data(patients_file)

    caregiver_locations = {}
    for crid, caregiver in caregivers_data.items():
        cid = caregiver['cid']
        if cid not in caregiver_locations:
            caregiver_locations[cid] = {
                'longitude': caregiver['longitude'],
                'latitude': caregiver['latitude']
            }

    patient_locations = {}
    for prid, patient in patients_data.items():
        pid = patient['pid']
        if pid not in patient_locations:
            patient_locations[pid] = {
                'longitude': patient['longitude'],
                'latitude': patient['latitude']
            }

    print(f"Found {len(caregiver_locations)} unique caregivers and {len(patient_locations)} unique patients")

    travel_methods = ['driving-car', 'cycling-regular', 'foot-walking']

    for travel_method in travel_methods:
        print(f"\n{'='*60}")
        print(f"Processing {travel_method}")
        print(f"{'='*60}")

        results = {}
        print("\n1. Computing caregiver to caregiver distances...")
        results['cid_to_cid'] = get_distance_matrix(
            entities_info1=caregiver_locations,
            entities_info2=caregiver_locations,
            travel_method=travel_method,
            step_size=step_size
        )
        print("\n2. Computing patient to patient distances...")
        results['pid_to_pid'] = get_distance_matrix(
            entities_info1=patient_locations,
            entities_info2=patient_locations,
            travel_method=travel_method,
            step_size=step_size
        )
        print("\n3. Computing caregiver to patient distances...")
        results['cid_to_pid'] = get_distance_matrix(
            entities_info1=caregiver_locations,
            entities_info2=patient_locations,
            travel_method=travel_method,
            step_size=step_size
        )
        print("\n4. Computing patient to caregiver distances...")
        results['pid_to_cid'] = get_distance_matrix(
            entities_info1=patient_locations,
            entities_info2=caregiver_locations,
            travel_method=travel_method,
            step_size=step_size
        )

        merged_distance = {}
        merged_duration = {}
        for key, data in results.items():
            for k, v in data['distance'].items():
                merged_distance[f"{k[0]}_{k[1]}"] = v
            for k, v in data['duration'].items():
                merged_duration[f"{k[0]}_{k[1]}"] = v

        output_data = {'distance': merged_distance, 'duration': merged_duration}

        if travel_method == 'driving-car':
            output_file = os.path.join(output_dir, "driving_data.json")
        elif travel_method == 'cycling-regular':
            output_file = os.path.join(output_dir, "cycling_data.json")
        else:
            output_file = os.path.join(output_dir, "walking_data.json")

        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)
        print(f"\nSaved results to {output_file}")

        print("\nValidating outputs...")
        validate_outputs(output_data, caregiver_locations, patient_locations)
        print("Validation complete!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="distance_migration.osrm")
    parser.add_argument("--caregivers", type=str, default="caregivers.json", help="Path to caregivers JSON file")
    parser.add_argument("--patients", type=str, default="patient.json", help="Path to patients JSON file")
    parser.add_argument("--output", type=str, default="output", help="Output directory")
    parser.add_argument("--step_size", type=int, default=50, help="Batch size for API calls")
    args = parser.parse_args()

    process_all_pairs(
        caregivers_file=args.caregivers,
        patients_file=args.patients,
        output_dir=args.output,
        step_size=args.step_size
    )