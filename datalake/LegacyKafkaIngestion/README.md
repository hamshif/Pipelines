This module was created to work with old Kafka versions
That use Zookeeper to manage client groups
There is one direct version which circumvents zookeeper group managements all together by using it's own offsets
And another indirect version that uses the zookeeper mechanism


cmd args
=

for KafkaActivityIngestor1
-
```
--bootstrap broker1.wielder.com:6667,broker2.wielder.com:6667,broker3.wielder.com:6667 --zookeeper zk1.wielder.com:2181,zk2.wielder.com:2181 --fs /tmp/poc/gs/ --master local --topics 423-BRJ-744 --bucket marketo-datalake-dev-qe_423-brj-744 -o json
```

for KafkaActivityIngestor
-
example with special zookeeper sub directory
```
--groupid groupid --bootstrap broker1.wielder.com:6667,broker2.wielder.com:6667,broker3.wielder.com:6667 --zookeeper zk1.wielder.com:2181,zk2.wielder.com:2181/kafka --fs /tmp/poc/gs/ --master local --topics 423-BRJ-744 --bucket marketo-datalake-dev-qe_423-brj-744 -o json
```