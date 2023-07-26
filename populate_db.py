import sqlite3
import gzip
import sys
import csv

# Path to the annotations file is first arugment, path to the db dir is second
annot_path = sys.argv[1]
db_path = sys.argv[2]

# Generate a path for the block db:


def block_db_path(db_path, chr, pos):
    # Extract the last digit of the position:
    pos_last_digit = pos[-1]
    # Generate the path:
    block_db_path = db_path + "/c" + chr + "_b" + pos_last_digit + ".db"
    return block_db_path

# Write row to db:


def write_to_db(row_dict, curr_block_db_path):
    # Connect to db:
    conn = sqlite3.connect(curr_block_db_path)
    c = conn.cursor()
    table_cols = columns.copy()
    table_cols.append('index_var')
    # Create a list of types for the columns, if "pos" appears in colomn then integer, else text:
    table_types = []
    for col in table_cols:
        # Check if the column name includes "pos" at any part of th string:
        if "pos" in col:
            table_types.append("INTEGER")
        else:
            table_types.append("TEXT")
    # Generate an index from the chr, grch38_pos, ref, alt:
    index_var = "chr" + row_dict['chr'] + ":" + row_dict['grch38_pos'] + \
        ":" + row_dict['ref'] + ":" + row_dict['alt']
    # Update the row dictionary with the index:
    row_dict['index_var'] = index_var
    # Create table "annot" if it doesnt extis, with "table_cols" as column names and "table_types" as their data types:
    c.execute("CREATE TABLE IF NOT EXISTS annot (" +
              ','.join([f'{col} {table_types[i]}' for i, col in enumerate(table_cols)]) + ")")
    # Create an index on the "index_var" column of the "annot" table
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS index_var_idx ON annot (index_var)")
    conn.commit()

    # Check if row already exists in db:
    c.execute("SELECT * FROM annot WHERE index_var=?", (index_var,))
    # If row does not exist, insert it:
    if c.fetchone() is None:
        c.execute("INSERT INTO annot (" + ','.join(table_cols) +
                  ") VALUES (" + ','.join(['?'] * len(table_cols)) + ")", tuple(row_dict.values()))
    # If row does exist, update it;
    else:
        # retrieve the row from the db:
        c.execute("SELECT * FROM annot WHERE index_var=?", (index_var,))
        old_row = c.fetchone()
        # Create a dictionary from the old row:
        old_row_dict = dict(zip(table_cols, old_row))
        # delete the old row from the db:
        c.execute("DELETE FROM annot WHERE index_var=?", (index_var,))
        conn.commit()
        # Concatenate the old and new "REVEL" scores, sep by comma:
        row_dict['REVEL'] = old_row_dict['REVEL'] + \
            "," + row_dict['REVEL']
        row_dict['Ensembl_transcriptid'] = old_row_dict['Ensembl_transcriptid'] + \
            "," + row_dict['Ensembl_transcriptid']
        # Insert the updated row into the db in place of the old row:
        c.execute("INSERT INTO annot (" + ','.join(table_cols) +
                  ") VALUES (" + ','.join(['?'] * len(table_cols)) + ")", tuple(row_dict.values()))
    # Commit changes:
    conn.commit()
    # Close connection:
    conn.close()


# Read annot_path:
columns = []
row_num = 0
with gzip.open(annot_path, 'rt') as f:
    reader = csv.reader(f)
    for row in reader:
        # Assign column names:
        if row_num == 0:
            columns = row
            row_num += 1
            continue
        else:
            # Turn row into dictionary:
            row_dict = dict(zip(columns, row))
            row_num += 1
            curr_block_db_path = block_db_path(
                db_path, row_dict['chr'], row_dict['grch38_pos'])
            # Skip if grch38_pos cant be case into int:
            try:
                int(row_dict['grch38_pos'])
            except: 
                continue
            # write to db:
            write_to_db(row_dict, curr_block_db_path)
            # Increase the row number count, print the row number count every 10000 rows, and continue:
            row_num += 1
            if row_num % 10000 == 0:
                print(row_num)
                continue
            else:
                continue
