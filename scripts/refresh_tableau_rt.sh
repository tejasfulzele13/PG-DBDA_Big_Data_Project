#!/bin/bash

# ==============================
# CONFIG
# ==============================
HDFS_PATH=/ipl/exports_csv/rt_runs_per_match
# Added a parent path variable for clarity
LOCAL_PARENT=~/ipl_exports_csv
LOCAL_PATH=~/ipl_exports_csv/rt_runs_per_match
SHARED_PATH=/home/talentum/shared/ipl_bigdata_project/tableau_files/ipl_exports_csv
FINAL_CSV_NAME=rt_runs_per_match.csv

echo "Step 1: Removing old local export..."
rm -rf $LOCAL_PATH

echo "Step 2: Fetching fresh data from HDFS..."
# --- FIX: Ensure the parent folder exists ---
mkdir -p $LOCAL_PARENT

hdfs dfs -get $HDFS_PATH $LOCAL_PARENT/

echo "Step 3: Removing old CSV from shared folder..."
rm -f $SHARED_PATH/$FINAL_CSV_NAME

echo "Step 4: Copying new CSV to shared folder..."
# This finds the part-file and renames it to your final name
cp $LOCAL_PATH/part-*.csv $SHARED_PATH/$FINAL_CSV_NAME

echo "DONE Tableau file updated"
