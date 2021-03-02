#!/bin/bash -e

rm -Rf chatnoir-mapfile-generator
git clone git@webis.uni-weimar.de:code-research/web-search/chatnoir/chatnoir-mapfile-generator.git chatnoir-mapfile-generator
cd chatnoir-mapfile-generator

./gradlew shadowJar

cd ..

echo "Start indexing..."

input_path="file:///mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/{redirects,snapshots}/*/*/*/*.warc.gz"

/opt/hadoop-2.7.1/bin/hadoop jar chatnoir-mapfile-generator/build/libs/chatnoir2-mapfile-generator-*-all.jar \
	de.webis.chatnoir2.mapfile_generator.app.MapFileGenerator \
	-prefix "clueweb09-in-wayback12" -input "${input_path}" -format "commoncrawl" \
	-output "sigir2021/clueweb09-in-wayback12/segment-00000" | tee -a job.log

if tail -n 100 job.log | grep -q "Job failed as tasks failed"; then
    echo "Job failed!" >&2
    exit 1
fi

