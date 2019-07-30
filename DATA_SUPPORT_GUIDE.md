# Neon Data Support Guide

## Supported Datastores

- Elasticsearch 6.4+

*Want us to support other datastores?  Let us know!  Send us an email at [neon-support@nextcentury.com](mailto:neon-support@nextcentury.com)*

## Elasticsearch 6 Guidelines

### Elasticsearch 6 Mapping Files

It's usually very important to load a mapping file associated with your data index into Elasticsearch BEFORE loading any data into that index.

If you HAVE loaded data before loading your mapping file, you'll either need to [reindex](https://www.elastic.co/guide/en/elasticsearch/reference/6.4/docs-reindex.html) your data index or delete the index and start over again.

More information about mapping files can be found on the [Elasticsearch website](https://www.elastic.co/guide/en/elasticsearch/reference/6.4/mapping.html).

#### Date Fields

Date fields should have the `format` `"yyyy-MM-dd||dateOptionalTime||E MMM d HH:mm:ss zzz yyyy"`.  For example:

```json
"timestamp": {
    "type": "date",
    "format": "yyyy-MM-dd||dateOptionalTime||E MMM d HH:mm:ss zzz yyyy"
}
```

Note that you may need to add an additional date format to the end of the `format` string (separated by two pipe characters `||`).  For example, if the dates in your data look like `12/25/2018 01:23:45`, you would use the following `format` string:

```json
"date_field": {
    "type": "date",
    "format": "yyyy-MM-dd||dateOptionalTime||E MMM d HH:mm:ss zzz yyyy||MM/dd/yyyy HH:mm:ss"
}
```

#### Keyword Fields

We recommend that any string field not containing document text (including news articles, social media posts, or any multi-sentence text field) should have the `type` `keyword`.  For example, fields of names, links, categories, and alphanumeric IDs should all have the `type` `keyword`.

```json
"name": {
    "type": "keyword"
}
```

#### Text Fields

Text fields should have the `fielddata` property set to `true`.  For example:

```json
"social_media_post": {
    "type": "text",
    "fielddata": true
}
```

## Troubleshooting

For questions, please email us at [neon-support@nextcentury.com](mailto:neon-support@nextcentury.com)
