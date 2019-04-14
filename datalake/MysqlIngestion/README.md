This module was created to work with old Kafka versions
That use Zookeeper to manage client groups
There is one direct version which circumvents zookeeper group managements all together by using it's own offsets
And another indirect version that uses the zookeeper mechanism


cmd args
=

for SqlIngestor
-
```
-e int --fs /tmp/poc/gs --master local -h lost.laazazel.org --db hellfire --tables purgatory -u admin --pswd admin```

