
```xml
{
  "type": "mongodb",
  "mode": "standalone",
  "connections": "mongodb://localhost:27017/",
  "enabled": true
}
```


SELECT t.* FROM mongodb.`aaa` t  LIMIT 5


SELECT t.* FROM mongodb.`{"dbName": "***","collectionName": "***","aggs": ["a","b"],"projection": ""}` t  LIMIT 5