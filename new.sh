#!/bin/bash

# Check if time variable arguments are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <start_time> <end_time>"
    echo "Example: $0 '2024-01-01 00:00:00' '2024-01-01 23:59:59'"
    exit 1
fi

# Assign input arguments to variables
START_TIME="$1"
END_TIME="$2"

# Impala connection details
IMPALA_HOST="route_to_impala"
IMPALA_PORT="port"
OUTPUT_FILE="./test.csv"
TABLE_NAME="table"

# Run the impala-shell command
impala-shell \
    -i "${IMPALA_HOST}:${IMPALA_PORT}" \
    --ssl \
    -B \
    -o "${OUTPUT_FILE}" \
    --print_header \
    --output_delimiter=',' \
    -q "SELECT * FROM ${TABLE_NAME} WHERE timestamp >= '${START_TIME}' AND timestamp <= '${END_TIME}'"

# Check if the command was successful
if [ $? -eq 0 ]; then
    echo "Query executed successfully. Output saved to ${OUTPUT_FILE}"
else
    echo "Error executing query"
    exit 1
fi
