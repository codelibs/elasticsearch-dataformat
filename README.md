Elasticsearch Data Format Plugin
========================

## Overview

Elasticsearch supports JSON/SMILE/YAML formats as a response of a search result.
This plugin adds other formats, such as CSV.

## Installation

TBD

## Supported Output Formats

### CSV

This plugin allows you to download data as CSV file.

    $ curl -o /tmp/test.csv -XGET "localhost:9200/_data/scroll?format=csv

### Others

TBD...
