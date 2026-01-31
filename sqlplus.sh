sqlplus -s $DB_STRING <<EOF > output.csv
SET MARKUP CSV ON QUOTE ON
SELECT * FROM your_table;
EXIT;
EOF
