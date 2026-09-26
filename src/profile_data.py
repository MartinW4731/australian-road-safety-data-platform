import pandas as pd


# ============================================================
# 1. ACCIDENT LOCATION
# ============================================================
# Grain:
# One row represents the location information for one accident.
#
# Candidate Primary Key:
# ACCIDENT_NO
# ============================================================

df_location = pd.read_csv(
    "Data/RAW/_published_safety_victoria_road_crash_data_ACCIDENT_LOCATION.csv",
    low_memory=False
)

print("\n================ ACCIDENT LOCATION ================\n")

print("Shape:", df_location.shape)
print("\nColumns:")
print(df_location.columns)

print("\nData types:")
print(df_location.dtypes)

print("\nUnique ACCIDENT_NO:")
print(df_location["ACCIDENT_NO"].nunique())

print("\nNULL ACCIDENT_NO:")
print(df_location["ACCIDENT_NO"].isna().sum())

print("\nUnique NODE_ID:")
print(df_location["NODE_ID"].nunique())

print("\nNULL NODE_ID:")
print(df_location["NODE_ID"].isna().sum())

print("\nExact duplicate rows:")
print(df_location.duplicated().sum())


# ============================================================
# 2. ACCIDENT
# ============================================================
# Grain:
# One row represents one road crash / accident.
#
# Candidate Primary Key:
# ACCIDENT_NO
# ============================================================

df_acc = pd.read_csv(
    "Data/RAW/_published_safety_victoria_road_crash_data_ACCIDENT.csv",
    low_memory=False
)

print("\n================ ACCIDENT ================\n")

print("Shape:", df_acc.shape)

print("\nColumns:")
print(df_acc.columns)

print("\nUnique ACCIDENT_NO:")
print(df_acc["ACCIDENT_NO"].nunique())

print("\nNULL ACCIDENT_NO:")
print(df_acc["ACCIDENT_NO"].isna().sum())

print("\nExact duplicate rows:")
print(df_acc.duplicated().sum())


# ============================================================
# 3. NODE
# ============================================================
# Data Quality Findings:
#
# - The raw NODE dataset contains exact duplicate rows.
#
# - ACCIDENT_NO alone is not unique.
#
# - NODE_ID alone is not unique.
#
# - ACCIDENT_NO + NODE_ID is still not unique after removing
#   exact duplicate rows.
#
# - Some records for the same ACCIDENT_NO + NODE_ID contain
#   conflicting attribute values, such as DEG_URBAN_NAME.
#
# Therefore, no reliable Primary Key has been assigned to the
# raw NODE dataset at this stage.
# ============================================================

df_node = pd.read_csv(
    "Data/RAW/_published_safety_victoria_road_crash_data_NODE.csv",
    low_memory=False
)

print("\n================ NODE ================\n")

print("Shape:", df_node.shape)

print("\nColumns:")
print(df_node.columns)

print("\nUnique ACCIDENT_NO:")
print(df_node["ACCIDENT_NO"].nunique())

print("\nNULL ACCIDENT_NO:")
print(df_node["ACCIDENT_NO"].isna().sum())

print("\nUnique NODE_ID:")
print(df_node["NODE_ID"].nunique())

print("\nNULL NODE_ID:")
print(df_node["NODE_ID"].isna().sum())

# Check exact duplicate rows in the raw dataset.
exact_node_duplicates = df_node.duplicated().sum()

print("\nExact duplicate rows:")
print(exact_node_duplicates)

# Create an in-memory deduplicated DataFrame.
# This DOES NOT modify the original raw CSV file.
df_node_clean = df_node.drop_duplicates()

print("\nShape after removing exact duplicates:")
print(df_node_clean.shape)

# Check whether ACCIDENT_NO + NODE_ID can uniquely identify
# a record after exact duplicates are removed.
node_key_duplicates = df_node_clean.duplicated(
    subset=["ACCIDENT_NO", "NODE_ID"]
).sum()

print("\nDuplicate ACCIDENT_NO + NODE_ID combinations after exact deduplication:")
print(node_key_duplicates)


# ============================================================
# 4. ROAD SURFACE CONDITION
# ============================================================
# Grain:
# One row represents one road surface condition recorded
# for an accident.
#
# Candidate Composite Primary Key:
# ACCIDENT_NO + SURFACE_COND_SEQ
#
# Foreign Key:
# ACCIDENT_NO -> ACCIDENT.ACCIDENT_NO
#
# Relationship:
# ACCIDENT 1:M ROAD_SURFACE_CONDITION
# ============================================================

df_surface = pd.read_csv(
    "Data/RAW/_published_safety_victoria_road_crash_data_ROAD_SURFACE_COND.csv",
    low_memory=False
)

print("\n================ ROAD SURFACE CONDITION ================\n")

print("Shape:", df_surface.shape)

print("\nColumns:")
print(df_surface.columns)

print("\nUnique ACCIDENT_NO:")
print(df_surface["ACCIDENT_NO"].nunique())

print("\nNULL ACCIDENT_NO:")
print(df_surface["ACCIDENT_NO"].isna().sum())

print("\nExact duplicate rows:")
print(df_surface.duplicated().sum())

# Validate the candidate composite primary key.
surface_key_duplicates = df_surface.duplicated(
    subset=["ACCIDENT_NO", "SURFACE_COND_SEQ"]
).sum()

print("\nDuplicate ACCIDENT_NO + SURFACE_COND_SEQ combinations:")
print(surface_key_duplicates)

# Referential Integrity Check:
# Find ROAD_SURFACE_CONDITION records whose ACCIDENT_NO
# does not exist in the ACCIDENT table.
orphan_surface = df_surface[
    ~df_surface["ACCIDENT_NO"].isin(df_acc["ACCIDENT_NO"])
]

print("\nOrphan ROAD_SURFACE_CONDITION records:")
print(orphan_surface.shape[0])


# ============================================================
# 5. ATMOSPHERIC CONDITION
# ============================================================
# Grain:
# One row represents one atmospheric condition recorded
# for an accident.
#
# Candidate Composite Primary Key:
# ACCIDENT_NO + ATMOSPH_COND_SEQ
#
# Foreign Key:
# ACCIDENT_NO -> ACCIDENT.ACCIDENT_NO
#
# Relationship:
# ACCIDENT 1:M ATMOSPHERIC_CONDITION
#
# Some accidents may not have an atmospheric condition record.
# ============================================================

df_atmospheric = pd.read_csv(
    "Data/RAW/_published_safety_victoria_road_crash_data_ATMOSPHERIC_COND.csv",
    low_memory=False
)

print("\n================ ATMOSPHERIC CONDITION ================\n")

print("Shape:", df_atmospheric.shape)

print("\nColumns:")
print(df_atmospheric.columns)

print("\nUnique ACCIDENT_NO:")
print(df_atmospheric["ACCIDENT_NO"].nunique())

print("\nNULL ACCIDENT_NO:")
print(df_atmospheric["ACCIDENT_NO"].isna().sum())

print("\nExact duplicate rows:")
print(df_atmospheric.duplicated().sum())

# Validate the candidate composite primary key.
atmospheric_key_duplicates = df_atmospheric.duplicated(
    subset=["ACCIDENT_NO", "ATMOSPH_COND_SEQ"]
).sum()

print("\nDuplicate ACCIDENT_NO + ATMOSPH_COND_SEQ combinations:")
print(atmospheric_key_duplicates)

# Referential Integrity Check:
# Find atmospheric records whose ACCIDENT_NO does not exist
# in the ACCIDENT table.
orphan_atmospheric = df_atmospheric[
    ~df_atmospheric["ACCIDENT_NO"].isin(df_acc["ACCIDENT_NO"])
]

print("\nOrphan ATMOSPHERIC_CONDITION records:")
print(orphan_atmospheric.shape[0])

# Reverse relationship check:
# Find accidents that do not have any atmospheric condition record.
missing_atmospheric = df_acc[
    ~df_acc["ACCIDENT_NO"].isin(df_atmospheric["ACCIDENT_NO"])
]

print("\nAccidents without ATMOSPHERIC_CONDITION records:")
print(missing_atmospheric.shape[0])


# ============================================================
# PROFILING COMPLETE
# ============================================================

print("\n======================================================")
print("DATA PROFILING COMPLETE")
print("======================================================\n")