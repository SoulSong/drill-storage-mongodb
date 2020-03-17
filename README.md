## Config Storage plugin in web-UI
```xml
{
  "type": "mongodb",
  "mode": "standalone",
  "connections": "localhost:27017",
}
```

## Test case
```sql
SELECT t.* FROM mongodb.`{"dbName": "test","collectionName": "collection_1","aggs": ["***","{ $unwind : '$ids' }","***"],"projection": "id,size"}` t  LIMIT 5
```


