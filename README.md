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
SELECT t.* FROM mongodb.`{"dbName": "test","collectionName": "classificationCol","aggs": [" { $match: { 'folderId':'cc54ae57-4cda-4aa1-85d3-de58cf8b1226'}}","{$project: { 'folderId': 1, pdocIds:1,classificationIds: { $split: ['$longClassificationId', '.'] } }}","{ $unwind : '$classificationIds' }","{ $unwind : '$pdocIds' }","{$group: {_id: {classificationId :'$classificationIds' },pdocIds: { $addToSet: '$pdocIds' }}}","{ $project:{'classificationId':'$_id.classificationId',size:{$size:'$pdocIds'},'_id':0}}"],"projection": "classificationId,size"}` t  LIMIT 5

```


