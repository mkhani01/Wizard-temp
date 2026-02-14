#!/usr/bin/env python3
"""
Migration Script: Client Hours with Service Type -> ClientAvailability

This script migrates client service hours from Excel to ClientAvailability records.
It handles:
- Template records (recurring schedules with 1970 dates) -> recurring ClientAvailability
- Actual records (specific dates in 2026) -> logged but NOT migrated

Field Mappings:
- Service Location Name -> client name (lookup in clients table, case-insensitive)
- Planned Start Date Weekday -> days (DayOfWeek enum)
- Planned Start/End Date And Time (Excel serial) -> requestedStartTime/requestedEndTime
- Service Requirement Duration -> duration (in minutes)

Pattern Detection:
- Analyzes gaps between occurrences to determine occursEvery (weekly, bi-weekly, etc.)
"""

import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import json
from typing import Optional, List, Dict, Any

# Constants
EXCEL_EPOCH = datetime(1899, 12, 30)
DAY_OF_WEEK_MAP = {
    'Monday': 'Monday',
    'Tuesday': 'Tuesday', 
    'Wednesday': 'Wednesday',
    'Thursday': 'Thursday',
    'Friday': 'Friday',
    'Saturday': 'Saturday',
    'Sunday': 'Sunday',
}


def excel_serial_to_time(serial) -> Optional[str]:
    """Convert Excel serial number to time string (HH:MM)"""
    if pd.isna(serial):
        return None
    time_part = float(serial) % 1
    total_minutes = int(time_part * 24 * 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    return f'{hours:02d}:{minutes:02d}'


def excel_serial_to_datetime(serial) -> Optional[datetime]:
    """Convert Excel serial number to datetime"""
    if pd.isna(serial):
        return None
    return EXCEL_EPOCH + timedelta(days=float(serial))


def time_to_minutes(time_str: str) -> int:
    """Convert HH:MM to minutes since midnight"""
    if not time_str:
        return 0
    h, m = map(int, time_str.split(':'))
    return h * 60 + m


def detect_recurrence_pattern(dates: List[datetime]) -> Dict[str, Any]:
    """
    Analyze dates to detect recurrence pattern.
    Returns dict with occursEvery and pattern description.
    
    Pattern detection logic:
    - Calculate gaps between consecutive dates
    - Filter out 0-day gaps (multiple slots on same day)
    - Most common non-zero gap should be multiple of 7 (weekly = 7, bi-weekly = 14, etc.)
    - occursEvery = most_common_gap / 7
    """
    if len(dates) < 2:
        return {'occursEvery': 1, 'pattern': 'weekly (assumed - single occurrence)', 'gaps': []}
    
    sorted_dates = sorted(dates)
    all_gaps = [(sorted_dates[i+1] - sorted_dates[i]).days for i in range(len(sorted_dates)-1)]
    
    # Filter out 0-day gaps (multiple slots on same day)
    gaps = [g for g in all_gaps if g > 0]
    
    # If all gaps are 0 (all dates are same), default to weekly
    if not gaps:
        return {
            'occursEvery': 1, 
            'pattern': 'weekly (multiple slots on same day)', 
            'gaps': all_gaps,
            'note': 'Multiple occurrences on same day, assuming weekly recurrence'
        }
    
    # Count gap frequencies (only non-zero gaps)
    gap_counts = defaultdict(int)
    for gap in gaps:
        gap_counts[gap] += 1
    
    # Find the most common gap
    most_common_gap = max(gap_counts.keys(), key=lambda g: gap_counts[g])
    
    # Check if the pattern is consistent (most common gap represents majority)
    total_gaps = len(gaps)
    most_common_count = gap_counts[most_common_gap]
    
    # Determine occursEvery based on gap
    if most_common_gap == 7:
        occurs_every = 1
        pattern = 'weekly'
    elif most_common_gap == 14:
        occurs_every = 2
        pattern = 'bi-weekly'
    elif most_common_gap == 21:
        occurs_every = 3
        pattern = 'tri-weekly'
    elif most_common_gap == 28:
        occurs_every = 4
        pattern = 'every 4 weeks'
    elif most_common_gap % 7 == 0:
        occurs_every = most_common_gap // 7
        pattern = f'every {occurs_every} weeks'
    else:
        # Irregular pattern - default to weekly
        occurs_every = 1
        pattern = f'weekly (irregular gaps: {set(gaps)})'
    
    return {
        'occursEvery': occurs_every, 
        'pattern': pattern, 
        'gaps': all_gaps,
        'non_zero_gaps': gaps,
        'gap_counts': dict(gap_counts),
        'consistency': f'{most_common_count}/{total_gaps} gaps are {most_common_gap} days'
    }


def process_excel_data(file_path: str) -> Dict[str, Any]:
    """
    Process the Excel file and extract availability data.
    """
    print(f"Reading Excel file: {file_path}")
    df = pd.read_excel(file_path, sheet_name='Data')
    
    print(f"Total rows: {len(df)}")
    
    # Filter for Personal Care
    filtered = df[
        (df['Actual Service Type Description'] == 'Personal Care') & 
        (df['Actual Service Requirement Type Description'] == 'Personal Care')
    ].copy()
    
    print(f"Filtered rows (Personal Care): {len(filtered)}")
    
    # Convert Service Requirement dates
    filtered['Service Requirement Start Date And Time'] = pd.to_datetime(
        filtered['Service Requirement Start Date And Time'], errors='coerce'
    )
    
    # Separate template records (recurring) from actual records (one-time)
    template_mask = filtered['Service Requirement Start Date And Time'].dt.year < 2026
    template_records = filtered[template_mask].copy()
    actual_records = filtered[~template_mask].copy()
    
    print(f"Template records (recurring): {len(template_records)}")
    print(f"Actual records (one-time): {len(actual_records)} - will be logged but NOT migrated")
    
    # Process template records (recurring schedules)
    template_records['start_time'] = template_records['Planned Start Date And Time'].apply(excel_serial_to_time)
    template_records['end_time'] = template_records['Planned End Date And Time'].apply(excel_serial_to_time)
    template_records['planned_datetime'] = template_records['Planned Start Date And Time'].apply(excel_serial_to_datetime)
    
    # Log actual records (not migrated)
    actual_records_list = []
    if len(actual_records) > 0:
        actual_records['start_time'] = pd.to_datetime(
            actual_records['Service Requirement Start Date And Time'], errors='coerce'
        ).dt.strftime('%H:%M')
        actual_records['end_time'] = pd.to_datetime(
            actual_records['Service Requirement End Date And Time'], errors='coerce'
        ).dt.strftime('%H:%M')
        actual_records['date'] = pd.to_datetime(
            actual_records['Service Requirement Start Date And Time'], errors='coerce'
        ).dt.date
        
        for _, row in actual_records.iterrows():
            if pd.isna(row.get('date')):
                continue
            actual_records_list.append({
                'client_name': row['Service Location Name'],
                'day': row['Planned Start Date Weekday'],
                'start_time': row['start_time'],
                'end_time': row['end_time'],
                'date': str(row['date'])
            })
    
    # Group template records by client, day, time to find patterns
    recurring_availabilities = []
    pattern_stats = defaultdict(int)
    
    for client in template_records['Service Location Name'].unique():
        client_data = template_records[template_records['Service Location Name'] == client]
        
        # Group by day and time
        grouped = client_data.groupby(['Planned Start Date Weekday', 'start_time', 'end_time']).agg({
            'planned_datetime': list,
            'Service Requirement Duration': 'first'
        }).reset_index()
        
        for _, row in grouped.iterrows():
            dates = [d for d in row['planned_datetime'] if d is not None]
            if not dates:
                continue
            
            pattern_info = detect_recurrence_pattern(dates)
            pattern_stats[pattern_info['pattern']] += 1
            
            # Calculate duration in minutes
            duration_hours = row['Service Requirement Duration'] or 1.0
            duration_minutes = int(duration_hours * 60)
            
            # Get start and end times
            start_time = row['start_time']
            end_time = row['end_time']
            
            # Use the first date as startDate for recurring pattern
            first_date = min(dates)
            
            recurring_availabilities.append({
                'client_name': client,
                'client_name_lower': client.lower(),  # For case-insensitive matching
                'day': row['Planned Start Date Weekday'],
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration_minutes,
                'startDate': first_date.strftime('%Y-%m-%d'),
                'occursEvery': pattern_info['occursEvery'],
                'pattern': pattern_info['pattern'],
                'isTemp': False,
                'occurrences': len(dates),
                'gaps': pattern_info.get('gaps', []),
                'consistency': pattern_info.get('consistency', 'N/A')
            })
    
    # Get unique client names
    all_clients = set(a['client_name'] for a in recurring_availabilities)
    
    return {
        'recurring_availabilities': recurring_availabilities,
        'actual_records_ignored': actual_records_list,
        'unique_clients': sorted(list(all_clients)),
        'pattern_stats': dict(pattern_stats),
        'stats': {
            'total_rows': len(df),
            'filtered_rows': len(filtered),
            'template_records': len(template_records),
            'actual_records': len(actual_records),
            'actual_records_ignored': len(actual_records_list),
            'recurring_slots': len(recurring_availabilities),
            'unique_clients': len(all_clients)
        }
    }


def generate_sql_migration(data: Dict[str, Any]) -> str:
    """
    Generate SQL migration statements.
    Uses case-insensitive matching for client names (LOWER() function).
    """
    sql_lines = []
    sql_lines.append("-- Migration: Client Hours with Service Type -> ClientAvailability")
    sql_lines.append(f"-- Generated: {datetime.now().isoformat()}")
    sql_lines.append(f"-- Total recurring slots: {len(data['recurring_availabilities'])}")
    sql_lines.append(f"-- Actual records ignored (logged separately): {len(data['actual_records_ignored'])}")
    sql_lines.append("")
    sql_lines.append("-- Pattern Statistics:")
    for pattern, count in sorted(data['pattern_stats'].items()):
        sql_lines.append(f"--   {pattern}: {count}")
    sql_lines.append("")
    
    # Create temp table for client lookup (case-insensitive)
    sql_lines.append("-- Create temp table for client name to ID mapping (case-insensitive)")
    sql_lines.append("CREATE TEMP TABLE client_name_map AS")
    sql_lines.append("SELECT id, name, LOWER(name) as name_lower FROM clients WHERE deleted_at IS NULL;")
    sql_lines.append("")
    sql_lines.append("-- Create index for faster lookups")
    sql_lines.append("CREATE INDEX idx_client_name_lower ON client_name_map(name_lower);")
    sql_lines.append("")
    
    # Group inserts by pattern for better readability
    slots_by_pattern = defaultdict(list)
    for slot in data['recurring_availabilities']:
        slots_by_pattern[slot['occursEvery']].append(slot)
    
    for occurs_every in sorted(slots_by_pattern.keys()):
        slots = slots_by_pattern[occurs_every]
        sql_lines.append(f"-- ============================================================")
        sql_lines.append(f"-- Recurring slots: occursEvery = {occurs_every} ({len(slots)} slots)")
        sql_lines.append(f"-- ============================================================")
        
        for slot in slots:
            days_array = f"ARRAY['{DAY_OF_WEEK_MAP.get(slot['day'], slot['day'])}']::dayofweek[]"
            client_name_escaped = slot['client_name'].replace("'", "''")
            client_name_lower = slot['client_name_lower'].replace("'", "''")
            
            sql = f"""INSERT INTO client_availabilities (
    client_id, days, requested_start_time, requested_end_time, 
    start_time, end_time, duration, 
    is_temp, is_unavailability, 
    start_date, occurs_every,
    number_of_care_givers, flex_start, flex_end, fix_window,
    created_date, last_modified_date
)
SELECT 
    c.id, 
    {days_array}, 
    '{slot['start_time']}', 
    '{slot['end_time']}',
    '{slot['start_time']}', 
    '{slot['end_time']}', 
    {slot['duration']},
    false, 
    false,
    '{slot['startDate']}'::date, 
    {slot['occursEvery']},
    1, 0, 0, false,
    NOW(), NOW()
FROM client_name_map c 
WHERE c.name_lower = '{client_name_lower}';"""
            
            sql_lines.append(sql)
            sql_lines.append("")
    
    sql_lines.append("-- Cleanup")
    sql_lines.append("DROP INDEX IF EXISTS idx_client_name_lower;")
    sql_lines.append("DROP TABLE client_name_map;")
    
    return "\n".join(sql_lines)


def generate_typescript_migration(data: Dict[str, Any]) -> str:
    """
    Generate TypeScript migration script for NestJS/TypeORM.
    Uses case-insensitive client name matching.
    """
    ts_lines = []
    ts_lines.append("import { MigrationInterface, QueryRunner } from 'typeorm';")
    ts_lines.append("")
    ts_lines.append(f"// Migration: Client Hours with Service Type -> ClientAvailability")
    ts_lines.append(f"// Generated: {datetime.now().isoformat()}")
    ts_lines.append(f"// Total recurring slots: {len(data['recurring_availabilities'])}")
    ts_lines.append(f"// Actual records ignored: {len(data['actual_records_ignored'])}")
    ts_lines.append("")
    ts_lines.append("// Pattern Statistics:")
    for pattern, count in sorted(data['pattern_stats'].items()):
        ts_lines.append(f"//   {pattern}: {count}")
    ts_lines.append("")
    ts_lines.append("export class MigrateClientHours1234567890 implements MigrationInterface {")
    ts_lines.append("  name = 'MigrateClientHours1234567890';")
    ts_lines.append("")
    ts_lines.append("  public async up(queryRunner: QueryRunner): Promise<void> {")
    ts_lines.append("    // Get all clients for lookup (case-insensitive)")
    ts_lines.append("    const clients = await queryRunner.query(`")
    ts_lines.append("      SELECT id, name, LOWER(name) as name_lower FROM clients WHERE deleted_at IS NULL")
    ts_lines.append("    `);")
    ts_lines.append("    const clientMap = new Map(clients.map((c: any) => [c.name_lower, c.id]));")
    ts_lines.append("")
    
    # Group by occursEvery for organized output
    slots_by_occurs = defaultdict(list)
    for slot in data['recurring_availabilities']:
        slots_by_occurs[slot['occursEvery']].append(slot)
    
    for occurs_every in sorted(slots_by_occurs.keys()):
        slots = slots_by_occurs[occurs_every]
        ts_lines.append(f"    // ========================================")
        ts_lines.append(f"    // occursEvery = {occurs_every} ({len(slots)} slots)")
        ts_lines.append(f"    // ========================================")
        ts_lines.append(f"    const slots{occurs_every}Week = [")
        
        for slot in slots[:200]:  # Limit for readability
            client_name_escaped = slot['client_name'].replace("\\", "\\\\").replace("'", "\\'")
            client_name_lower_escaped = slot['client_name_lower'].replace("\\", "\\\\").replace("'", "\\'")
            ts_lines.append(f"      {{")
            ts_lines.append(f"        clientName: '{client_name_escaped}',")
            ts_lines.append(f"        clientNameLower: '{client_name_lower_escaped}',")
            ts_lines.append(f"        day: '{slot['day']}',")
            ts_lines.append(f"        startTime: '{slot['start_time']}',")
            ts_lines.append(f"        endTime: '{slot['end_time']}',")
            ts_lines.append(f"        duration: {slot['duration']},")
            ts_lines.append(f"        startDate: '{slot['startDate']}',")
            ts_lines.append(f"        occursEvery: {slot['occursEvery']},")
            ts_lines.append(f"        pattern: '{slot['pattern']}',")
            ts_lines.append(f"      }},")
        
        if len(slots) > 200:
            ts_lines.append(f"      // ... and {len(slots) - 200} more slots")
        
        ts_lines.append("    ];")
        ts_lines.append("")
        ts_lines.append(f"    for (const slot of slots{occurs_every}Week) {{")
        ts_lines.append("      const clientId = clientMap.get(slot.clientNameLower);")
        ts_lines.append("      if (!clientId) {")
        ts_lines.append("        console.warn(`[MIGRATION] Client not found: ${slot.clientName}`);")
        ts_lines.append("        continue;")
        ts_lines.append("      }")
        ts_lines.append("")
        ts_lines.append("      await queryRunner.query(`")
        ts_lines.append("        INSERT INTO client_availabilities (")
        ts_lines.append("          client_id, days, requested_start_time, requested_end_time,")
        ts_lines.append("          start_time, end_time, duration,")
        ts_lines.append("          is_temp, is_unavailability,")
        ts_lines.append("          start_date, occurs_every,")
        ts_lines.append("          number_of_care_givers, flex_start, flex_end, fix_window,")
        ts_lines.append("          created_date, last_modified_date")
        ts_lines.append("        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW())")
        ts_lines.append("      `, [")
        ts_lines.append("        clientId,")
        ts_lines.append("        [slot.day],")
        ts_lines.append("        slot.startTime,")
        ts_lines.append("        slot.endTime,")
        ts_lines.append("        slot.startTime,")
        ts_lines.append("        slot.endTime,")
        ts_lines.append("        slot.duration,")
        ts_lines.append("        false, // is_temp")
        ts_lines.append("        false, // is_unavailability")
        ts_lines.append("        slot.startDate,")
        ts_lines.append("        slot.occursEvery,")
        ts_lines.append("        1, // number_of_care_givers")
        ts_lines.append("        0, // flex_start")
        ts_lines.append("        0, // flex_end")
        ts_lines.append("        false, // fix_window")
        ts_lines.append("      ]);")
        ts_lines.append("    }")
        ts_lines.append("")
    
    ts_lines.append("    console.log('[MIGRATION] Client availability migration completed');")
    ts_lines.append("  }")
    ts_lines.append("")
    ts_lines.append("  public async down(queryRunner: QueryRunner): Promise<void> {")
    ts_lines.append("    // This migration would need migration_id to properly rollback")
    ts_lines.append("    console.warn('[MIGRATION] Rollback not fully supported - manual cleanup required');")
    ts_lines.append("  }")
    ts_lines.append("}")
    
    return "\n".join(ts_lines)


def generate_json_output(data: Dict[str, Any]) -> str:
    """Generate JSON output for the migration data."""
    return json.dumps(data, indent=2, default=str)


def generate_log_report(data: Dict[str, Any]) -> str:
    """Generate a human-readable log report."""
    lines = []
    lines.append("=" * 70)
    lines.append("CLIENT AVAILABILITY MIGRATION REPORT")
    lines.append("=" * 70)
    lines.append(f"Generated: {datetime.now().isoformat()}")
    lines.append("")
    
    # Stats
    lines.append("SUMMARY STATS")
    lines.append("-" * 40)
    for key, value in data['stats'].items():
        lines.append(f"  {key}: {value}")
    lines.append("")
    
    # Pattern statistics
    lines.append("PATTERN DETECTION RESULTS")
    lines.append("-" * 40)
    for pattern, count in sorted(data['pattern_stats'].items()):
        lines.append(f"  {pattern}: {count}")
    lines.append("")
    
    # Sample recurring slots by pattern
    lines.append("SAMPLE RECURRING SLOTS BY PATTERN")
    lines.append("-" * 40)
    
    seen_patterns = set()
    for slot in data['recurring_availabilities']:
        if slot['pattern'] not in seen_patterns:
            seen_patterns.add(slot['pattern'])
            lines.append(f"")
            lines.append(f"Pattern: {slot['pattern']}")
            lines.append(f"  Client: {slot['client_name']}")
            lines.append(f"  Day: {slot['day']}")
            lines.append(f"  Time: {slot['start_time']} - {slot['end_time']}")
            lines.append(f"  Duration: {slot['duration']} minutes")
            lines.append(f"  Start Date: {slot['startDate']}")
            lines.append(f"  Occurs Every: {slot['occursEvery']} week(s)")
            lines.append(f"  Occurrences: {slot['occurrences']}")
            lines.append(f"  Gaps: {slot['gaps']}")
            lines.append(f"  Consistency: {slot['consistency']}")
    
    # Actual records ignored
    lines.append("")
    lines.append("=" * 70)
    lines.append("ACTUAL RECORDS IGNORED (NOT MIGRATED)")
    lines.append("=" * 70)
    lines.append(f"Total: {len(data['actual_records_ignored'])}")
    lines.append("")
    
    for i, record in enumerate(data['actual_records_ignored'][:50]):  # Show first 50
        lines.append(f"  [{i+1}] {record['client_name']}")
        lines.append(f"      Date: {record['date']}, Day: {record['day']}")
        lines.append(f"      Time: {record['start_time']} - {record['end_time']}")
    
    if len(data['actual_records_ignored']) > 50:
        lines.append(f"  ... and {len(data['actual_records_ignored']) - 50} more")
    
    return "\n".join(lines)


def main():
    import sys
    
    file_path = '/home/z/my-project/upload/Client Hours with Service Type.xlsx'
    output_dir = '/home/z/my-project/download/'
    
    print("=" * 70)
    print("Client Hours Migration Script")
    print("=" * 70)
    
    # Process data
    data = process_excel_data(file_path)
    
    # Print stats
    print("\nStats:")
    for key, value in data['stats'].items():
        print(f"  {key}: {value}")
    
    # Print pattern statistics
    print("\nPattern Detection Results:")
    for pattern, count in sorted(data['pattern_stats'].items()):
        print(f"  {pattern}: {count}")
    
    # Generate outputs
    print("\nGenerating outputs...")
    
    # SQL output
    sql_content = generate_sql_migration(data)
    sql_path = f"{output_dir}client_availability_migration.sql"
    with open(sql_path, 'w') as f:
        f.write(sql_content)
    print(f"  SQL migration: {sql_path}")
    
    # TypeScript output
    ts_content = generate_typescript_migration(data)
    ts_path = f"{output_dir}client_availability_migration.ts"
    with open(ts_path, 'w') as f:
        f.write(ts_content)
    print(f"  TypeScript migration: {ts_path}")
    
    # JSON output
    json_content = generate_json_output(data)
    json_path = f"{output_dir}client_availability_data.json"
    with open(json_path, 'w') as f:
        f.write(json_content)
    print(f"  JSON data: {json_path}")
    
    # Log report
    log_content = generate_log_report(data)
    log_path = f"{output_dir}migration_log.txt"
    with open(log_path, 'w') as f:
        f.write(log_content)
    print(f"  Log report: {log_path}")
    
    # Print actual records to console (logged but not migrated)
    print("\n" + "=" * 70)
    print("ACTUAL RECORDS IGNORED (logged, not migrated):")
    print("=" * 70)
    for record in data['actual_records_ignored'][:20]:
        print(f"  {record['client_name']}: {record['date']} ({record['day']}) {record['start_time']}-{record['end_time']}")
    if len(data['actual_records_ignored']) > 20:
        print(f"  ... and {len(data['actual_records_ignored']) - 20} more")
    
    print("\n" + "=" * 70)
    print("Migration complete!")
    print("=" * 70)


if __name__ == '__main__':
    main()
