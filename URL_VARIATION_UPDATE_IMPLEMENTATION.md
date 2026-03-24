# PDP Input Creation - URL & Variation Update Implementation

## Overview

This implementation provides two modes for PDP input creation:

1. **Normal Mode** - Standard PDP input creation (existing behavior)
2. **URL & Variation Update Mode** - Updates URLs and variations from mapping database

---

## Mode 1: Normal PDP Input Creation Script

**File**: `pdp_input_creation_script.py`

### Usage

```bash
python3 pdp_input_creation_script.py \
  --root /path/to/root \
  --business-file /path/to/business/file.xlsx \
  --cp-file /path/to/cp/input.tsv \
  --go-live-date 2026-03-24
```

### Parameters

- `--root`: Root folder (defaults to script directory)
- `--business-file`: Business team file (optional - uses latest from BUSSINESS TEAM PDP FILE folder)
- `--cp-file`: CP tool input file (optional - uses latest from CP TOOL INPUT folder)
- `--go-live-date`: Go-live date in YYYY-MM-DD format (optional - defaults to today)
- `--mapping-file`: **NEW** - Optional mapping database file for URL and variation updates

### Enhanced with Mapping Database Support

The script now optionally accepts a mapping database file (`--mapping-file`) which:
- Reads the "DB" sheet from the mapping Excel file
- Matches SKUs by (Base_ID, Scope, Retailer)
- Checks the Status column for update indicators
- Applies updated URLs and variations based on status:
  - `"URL Updated"` → Updates only URL
  - `"Variation Updated"` → Updates only variations
  - `"URL, Variation Updated"` → Updates both

### Example with Mapping Database

```bash
python3 pdp_input_creation_script.py \
  --business-file business_data.xlsx \
  --mapping-file Internal_COTY_Benelux_Consumer Beauty_Database_DAMA.xlsx
```

---

## Mode 2: URL & Variation Update Script

**File**: `pdp_url_variation_update_script.py`

### Purpose

This script is dedicated to **bulk URL and variation updates** from the mapping database file. It's used when the dashboard toggle is set to "URL & Variation Update".

### Usage

```bash
python3 pdp_url_variation_update_script.py \
  --root /path/to/root \
  --cp-file /path/to/cp/input.tsv \
  --mapping-file /path/to/mapping/database.xlsx \
  --go-live-date 2026-03-24
```

### Parameters

- `--root`: Root folder (defaults to script directory)
- `--cp-file`: CP tool input file (optional - uses latest from CP TOOL INPUT folder)
- `--mapping-file`: Mapping database file (required for this mode)
- `--go-live-date`: Go-live date in YYYY-MM-DD format (optional - defaults to today)

### Processing Logic

1. **Matches SKUs**: (Base_ID, Scope, Retailer) from CP input to mapping database
2. **Checks Status Column**: Each matched row in mapping database
3. **Applies Updates**:
   - If Status contains `"URL Updated"`:
     - Updates only the URL column
   - If Status contains `"Variation Updated"`:
     - Updates Variation_1, Variation_2, Variation_3 columns
   - If Status contains `"URL, Variation Updated"`:
     - Updates both URL and all variation columns
4. **Uses Updated Columns**: 
   - `Updated URL` → for URL updates
   - `Updated Variation 1/2/3` → for variation updates
5. **Preserves Other Data**: All other columns remain unchanged

### Output

- Creates TSV file with updated values
- Files are saved in the OUTPUT folder
- Filename includes date and "_URLVariationUpdate" suffix

### Example Output

```
OUTPUT/
  LVMH_CP_CP_TBL_LVMH_PDP_INPUT_Template_URLVariationUpdate_20260324.tsv
```

---

## Integration with Dashboard UI

### Toggle Logic

**IF toggle = OFF (Normal Mode)**
```
Use: pdp_input_creation_script.py
(Optional: with --mapping-file for automatic URL/variation lookup)
```

**IF toggle = ON (URL & Variation Update Mode)**
```
Use: pdp_url_variation_update_script.py --mapping-file <file>
```

### Implementation in Backend

1. **Frontend Toggle**: Add toggle UI element
   - Label: "URL & Variation Update"
   - Values: ON/OFF

2. **Backend Route Handler**:
```python
@app.post("/process_pdp")
def process_pdp(request):
    url_variation_update_mode = request.json.get("url_variation_update", False)
    
    if url_variation_update_mode:
        # Use URL & Variation Update script
        subprocess.run([
            "python3", 
            "pdp_url_variation_update_script.py",
            "--mapping-file", mapping_file_path,
            "--cp-file", cp_file_path
        ])
    else:
        # Use Normal PDP Input Creation script
        subprocess.run([
            "python3",
            "pdp_input_creation_script.py",
            "--business-file", business_file_path,
            "--cp-file", cp_file_path
        ])
```

---

## Mapping Database File Structure

**File**: `Internal_COTY_Benelux_Consumer Beauty_Database_DAMA.xlsx`

### Required Columns

| Column | Purpose |
|--------|---------|
| `Base_ID` | Unique SKU identifier |
| `Scope` | Scope/Partner identifier |
| `Retailer` | Retailer name |
| `Status` | Update indicator (e.g., "Update", "URL Updated", "Variation Updated", etc.) |
| `Updated URL` | New retailer URL (if URL needs update) |
| `Updated Variation 1` | New variation 1 (if variation needs update) |
| `Updated Variation 2` | New variation 2 (if variation needs update) |
| `Updated Variation 3` | New variation 3 (if variation needs update) |

### Status Column Values

- `"No Match"` - Skip, no update
- `"Update"` - Generic update flag
- `"URL Updated"` - Update URL only
- `"Variation Updated"` - Update variations only
- `"URL, Variation Updated"` - Update both URL and variations

---

## Processing Summary

### Script 1 (Normal PDP Input Creation)

**Inputs**:
- Business file (new SKUs to add)
- CP input file (existing SKUs)
- Optional: Mapping database

**Process**:
1. Merge business + CP data
2. Create new rows for business SKUs
3. If mapping file provided: lookup & apply URL/variation updates
4. Combine with existing CP data

**Output**:
- Merged CP input file with new rows
- Enhanced with mapped URL/variations if available

### Script 2 (URL & Variation Update)

**Inputs**:
- CP input file (existing SKUs)
- Mapping database (required)

**Process**:
1. Load CP input file
2. Load mapping database (DB sheet)
3. Match each CP row with mapping row
4. Check Status column for update type
5. Apply corresponding URL/variation updates
6. Preserve all other columns

**Output**:
- Updated CP input file
- Only rows with status="Update" or similar are modified

---

## Testing

### Test Normal Mode
```bash
python3 pdp_input_creation_script.py
```

### Test with Mapping File
```bash
python3 pdp_input_creation_script.py \
  --mapping-file Internal_COTY_Benelux_Consumer Beauty_Database_DAMA.xlsx
```

### Test URL & Variation Update Mode
```bash
python3 pdp_url_variation_update_script.py \
  --mapping-file Internal_COTY_Benelux_Consumer Beauty_Database_DAMA.xlsx
```

---

## File Locations

```
/Users/kunalpuri/Developer/pl_runner/
├── pdp_input_creation_script.py  (Enhanced with --mapping-file)
├── pdp_url_variation_update_script.py  (NEW - dedicated update script)
├── BUSSINESS TEAM PDP FILE/  (Input folder)
├── CP TOOL INPUT/  (Input folder)
└── OUTPUT/  (Output folder)
```

---

## Error Handling

Both scripts include error handling for:
- Missing files
- Invalid column mappings
- Empty DataFrames
- Invalid date formats
- Missing required columns

Check console output for warnings and error messages.
