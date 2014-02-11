Elasticsearch Data Format Plugin
========================

## Overview

Elasticsearch Data Format Plugin provides a feature to allow you to download a response of a search result as several formats other than JSON.
The supported formats are CSV, Excel and JSON(Bulk).

## Version

| River Web | elasticsearch |
|:---------:|:-------------:|
| master    | 1.0.0.X       |
| 0.2.0     | 1.0.0.RC2     |
| 0.1.0     | 0.90.7        |

## Installation

    $ $ES_HOME/bin/plugin --install org.codelibs/elasticsearch-dataformat/0.2.0

## Supported Output Formats

This plugin allows you to download data as a format you want.
Parameters for a query are the same as a scan query.

### CSV

    $ curl -o /tmp/data.csv -XGET "localhost:9200/_data?format=csv&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| fl                | string  |             |
| source            | string  |             |
| csv.separator     | string  |             |
| csv.quote         | string  |             |
| csv.escape        | string  |             |
| csv.nullString    | string  |             |
| append.header     | boolean |             |
| csv.encoding      | string  |             |

### Excel

    $ curl -o /tmp/data.xls -XGET "localhost:9200/_data?format=xls&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| source            | string  |             |

### JSON (Elasticsearch Bulk format)

    $ curl -o /tmp/data.json -XGET "localhost:9200/_data?format=json&source=..."

| Request Parameter | Type    | Description |
|:------------------|:-------:|:------------|
| source            | string  |             |
