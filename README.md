# Quick Start Apache Beam

Use `virtualenv`:

```
sudo apt install virtualenv
virtualenv -p python3.7 venv
source venv/bin/activate
```

Run Examples:

- Local Batch:
```
python main.py --n-words 500 --runner DirectRunner --input data/ggm_cas.txt --output data/output
```

- GCP Batch:
```
python main.py \
 --runner DataflowRunner \
 --n-words 500 \
 --in-file gs://BUCKET_NAME/ggm_cas.txt \
 --out-file gs://BUCKET_NAME/outfile \
 --project PROJECT_ID \
 --region us-east1 \
 --temp_location gs://BUCKET_NAME/tmp
```

- Local Streaming:
```
python stream.py --runner DirectRunner --input projects/PROJECT_ID/topics/TOPIC_NAME --output PROJECT_ID:DATASET_NAME.TABLE_NAME --streaming
```

- GCP Streaming:
```
python stream.py \
 --runner DataflowRunner \
 --input projects/PROJECT_ID/topics/TOPIC_NAME \
 --output PROJECT_ID:DATASET_NAME.TABLE_NAME \
 --project PROJECT_ID \
 --region us-east1 \
 --temp_location gs://BUCKET_NAME/tmp \
 --streaming
```