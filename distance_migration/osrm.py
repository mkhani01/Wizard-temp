"""
OSRM table API client: distance and duration matrices for driving, cycling, walking.
"""

import os
import sys
from typing import List, Dict, Literal, Tuple
from collections import defaultdict
import argparse
import json
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

OSRM_BASE_URL = 'https://osrm.caspianbmp.ie'


def call_osrm_table_api(
    locations: List[Tuple[float, float]],
    travel_method: Literal['driving-car', 'cycling-regular', 'foot-walking'],
    sources: List[int] = None,
    destinations: List[int] = None
) -> Dict:
    """
    Calls OSRM table API to get distance and duration matrix.

    Args:
        locations: List of (lon, lat) tuples - IMPORTANT: OSRM uses lon,lat format
        travel_method: One of 'driving-car', 'cycling-regular', 'foot-walking'
        sources: Optional list of source indices (default: all)
        destinations: Optional list of destination indices (default: all)

    Returns:
        Dictionary with 'distances' and 'durations' matrices
    """
    assert travel_method in OSRM_CONFIG, f"Unknown travel method: {travel_method}"

    config = OSRM_CONFIG[travel_method]
    base_path = config['base_path']

    # Format coordinates as lon,lat;lon,lat;...
    coordinates_str = ';'.join([f"{lon},{lat}" for lon, lat in locations])

    # Build URL - coordinates go in the path
    url = f"{OSRM_BASE_URL}{base_path}/{coordinates_str}"

    # Add query parameters
    params = {'annotations': 'distance,duration'}
    if sources is not None:
        params['sources'] = ';'.join(map(str, sources))
    if destinations is not None:
        params['destinations'] = ';'.join(map(str, destinations))

    try:
        response = requests.get(url, params=params, timeout=300)
        response.raise_for_status()
        data = response.json()

        if data.get('code') != 'Ok':
            raise ValueError(f"OSRM API error: {data.get('message', 'Unknown error')}")

        return {
            'distances': data.get('distances', []),
            'durations': data.get('durations', [])
        }
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to call OSRM API: {str(e)}")


def get_cross_distance_matrix(
    ent1: Dict,
    ent2: Dict,
    travel_method: Literal['driving-car', 'cycling-regular', 'foot-walking']
) -> Dict:
    """
    Computes distance matrix from ent1 → ent2 (rectangular matrix).

    Args:
        ent1: Dictionary of entities with 'longitude' and 'latitude' keys
        ent2: Dictionary of entities with 'longitude' and 'latitude' keys
        travel_method: Travel method to use

    Returns:
        Dictionary with 'distance' and 'duration' keys containing dictionaries
        mapping (entity1_id, entity2_id) tuples to values
    """
    assert travel_method in ('driving-car', 'cycling-regular', 'foot-walking')

    ent1_ids = list(ent1.keys())
    ent2_ids = list(ent2.keys())

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
            if eid == pid:
                distance[(eid, pid)] = 0.0
                duration[(eid, pid)] = 0
                continue

            dist = matrix['distances'][i][j]
            du = matrix['durations'][i][j]

            if dist is None or dist < 0:
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


def get_distance_matrix(
    entities_info1: Dict,
    entities_info2: Dict,
    travel_method: Literal['driving-car', 'cycling-regular', 'foot-walking'],
    step_size: int = 50
) -> Dict:
    """
    Computes distance matrix between two sets of entities.

    Args:
        entities_info1: Dictionary of entities with 'longitude' and 'latitude' keys
        entities_info2: Dictionary of entities with 'longitude' and 'latitude' keys
        travel_method: Travel method to use
        step_size: Batch size for API calls

    Returns:
        Dictionary with 'distance' and 'duration' keys
    """
    entity_ids1 = list(entities_info1.keys())
    num_entity1 = len(entities_info1)
    entity_ids2 = list(entities_info2.keys())
    num_entity2 = len(entities_info2)

    blocks1 = [(i, i+step_size) for i in range(0, num_entity1, step_size)]
    blocks2 = [(i, i+step_size) for i in range(0, num_entity2, step_size)]

    distance_info = {'distance': {}, 'duration': {}}

    if entities_info1 is entities_info2:
        for eid in entity_ids1:
            distance_info['distance'][(eid, eid)] = 0.0
            distance_info['duration'][(eid, eid)] = 0

    print(f"Processing {num_entity1} x {num_entity2} matrix with {travel_method}...")
    total_blocks = len(blocks1) * len(blocks2)
    current_block = 0

    for blk1 in blocks1:
        src_entities = {
            entity_ids1[i]: entities_info1[entity_ids1[i]]
            for i in range(blk1[0], min(blk1[1], num_entity1))
        }
        for blk2 in blocks2:
            dst_entities = {
                entity_ids2[j]: entities_info2[entity_ids2[j]]
                for j in range(blk2[0], min(blk2[1], num_entity2))
            }
            current_block += 1
            print(f"  Processing block {current_block}/{total_blocks}...")

            blk_distance = get_cross_distance_matrix(
                ent1=src_entities,
                ent2=dst_entities,
                travel_method=travel_method
            )
            distance_info['distance'].update(blk_distance['distance'])
            distance_info['duration'].update(blk_distance['duration'])

    return distance_info


def load_json_data(file_path: str) -> Dict:
    """Load JSON data from file."""
    with open(file_path, 'r') as f:
        return json.load(f)


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
