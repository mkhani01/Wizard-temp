# Availability Types Migration

Seeds the `availability_types` table from CSV to match the NestJS `AvailabilityType` entity.

## CSV format

Expected columns (e.g. `availabilityTypes.csv`):

| Column       | Required | Maps to   | Example values                    |
|-------------|----------|-----------|------------------------------------|
| Name        | Yes      | `name`    | Core, Office availability          |
| Type        | Yes      | `type`    | Availability, Unavailability       |
| Category    | No       | `category`| User, CLIENT, BOTH (default: USER) |
| Description | No       | `description` | Optional text                  |
| Is Paid     | No       | `is_paid` | YES / NO (default: false)          |
| Color       | No       | `color`   | #22bb33                            |
| Icon        | No       | `icon`    | Optional icon name                 |

- **Type** is normalized to `availability` or `unavailability`.
- **Category** is normalized to `USER`, `CLIENT`, or `BOTH`.

## How to run

**From the Migration Wizard**  
Select “Availability types” and choose the folder that contains your CSV(s). The wizard copies files into `assets/availabilitytypes` and runs this migration.

**From the command line**

```bash
# Use default folder assets/availabilitytypes (place CSV there first)
python main.py availability-types

# Or pass a CSV file or folder
python main.py availability-types /path/to/availabilityTypes.csv
python main.py availability-types /path/to/folder/with/csvs
```

Or run the module directly:

```bash
python -m availabilityTypeMigration
python -m availabilityTypeMigration /path/to/availabilityTypes.csv
```

## Database

Requires PostgreSQL with the `availability_types` table and a **unique constraint** on `(name, type, category)` for upsert. Columns: `name`, `type`, `category`, `description`, `color`, `icon`, `is_paid`, `created_date`, `last_modified_date`, `deleted_at`.

Set `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` (e.g. in `.env`).
