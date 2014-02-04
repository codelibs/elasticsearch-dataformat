Elasticsearch Data Format Plugin
========================

## Overview

Elasticsearch supports JSON/SMILE/YAML formats as the response of a search result.
This plugin adds supports for other formats, such as CSV and Excel.

## Version

| River Web | elasticsearch |
|:---------:|:-------------:|
| master    | 1.0.0.X       |
| 0.1.0     | 0.90.7        |

## Installation

    $ $ES_HOME/bin/plugin -install org.codelibs/elasticsearch-dataformat/0.1.0

## Supported Output Formats

This plugin allows you to download data as a format you want.
Parameters for a query are the same as a scan query.

### CSV

    $ curl -o /tmp/data.csv -XGET "localhost:9200/_data?format=csv"

### Excel

    $ curl -o /tmp/data.xls -XGET "localhost:9200/_data?format=excel"

### JSON (Elasticsearch Bulk format)

    $ curl -o /tmp/data.json -XGET "localhost:9200/_data?format=json"

