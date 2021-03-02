#!/bin/bash -e

#/usr/bin/curl -XPUT -H "Content-Type: application/json" "http://betaweb022:9200/_cluster/settings" -d '{"persistent": {"cluster.routing.allocation.disk.watermark.low":"500g","cluster.routing.allocation.disk.watermark.flood_stage":"5g","cluster.routing.allocation.disk.watermark.high":"300g"}}'

#/usr/bin/curl -XPUT -H "Content-Type: application/json" "http://betaweb022:9200/_cluster/settings" -d '{"persistent": {"cluster.routing.allocation.disk.watermark.low":"5g","cluster.routing.allocation.disk.watermark.flood_stage":"1g","cluster.routing.allocation.disk.watermark.high":"3g"}}'


#/usr/bin/curl -XPUT -H "Content-Type: application/json" "http://betaweb022:9200/_all/_settings?master_timeout=60s" -d '{"index.blocks.read_only_allow_delete": false}'

#/usr/bin/curl -XPUT -H 'Content-Type: application/json' 'http://betaweb022:9200/webis_warc_clueweb12_011/_settings?master_timeout=60s' -d '{"index": {"blocks.read_only_allow_delete": false,"blocks.read_only": false}}'
#/usr/bin/curl -XPUT -H 'Content-Type: application/json' 'http://betaweb022:9200/clueweb09-in-wayback12/_settings?master_timeout=60s' -d '{"index": {"blocks.read_only_allow_delete": false,"blocks.read_only": false}}'

curl -XPUT 'http://betaweb022:9200/webis_warc_clueweb12_011,clueweb09-in-wayback12/_alias/clueweb12-and-wayback12?master_timeout=60s'

