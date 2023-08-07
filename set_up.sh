annotations_path=${scratch}/sqlite_revel/revel_with_transcript_ids.csv.gz
db_dir=${scrath}/sqlite_revel/revel

mkdir $db_dir

python $s1/sqlite_annotations/populate_db.py $annotations_path $db_dir

# Add the revel directory to a compressez tar.gz archive:
tar -czvf sqlite_revel.tar.gz revel