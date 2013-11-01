Elasticsearch Data Format Plugin
========================

## Overview

Elasticsearch supports JSON/SMILE/YAML formats as a response of a search result.
This plugin adds other formats, such as CSV.

## Installation

    $ $ES_HOME/bin/plugin -install dataformat --url https://.../elasticsearch-dataformat...zip (see below)

ZIP file for Data Format plugin is in [HERE](https://oss.sonatype.org/content/repositories/snapshots/org/codelibs/elasticsearch-dataformat/).

## Supported Output Formats

### CSV

This plugin allows you to download data as CSV file.

    $ curl -o /tmp/test.csv -XGET "localhost:9200/_data?format=csv"

### Others

TBD...
