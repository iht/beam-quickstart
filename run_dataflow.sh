#!/bin/sh

python main.py \
  --input gs://ihr-beam-quickstart-dupe/data/el_quijote.txt \
  --output gs://ihr-beam-quickstart-dupe/out/ \
  --n-words 200 \
  --runner dataflow \
  --project vivid-art-312009 \
  --region europe-west2 \
  --temp_location gs://ihr-beam-quickstart-dupe/tmp/