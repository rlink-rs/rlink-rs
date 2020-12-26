Planning...

```sql
create table SourceTable (
    `rowtime` u64,
    `a` String,
    `b` String,
    `number` bigint,
    rowtime FOR order_time AS order_time - INTERVAL '5' SECOND,
) with (
    'connector' = 'kafka',
    'topic' = 'a',
    'broker-servers' = '192.168.1.1:9200'
    'startup-mode' = 'earliest-offset',
    'decode-mode' = 'java|json',
    'decode-java-class' = 'x.b.C',
);

create table SinkTable (
    `ss` bigint,
) with (
    'connector' = 'kafka',
    'topic' = 'a',
    'broker-servers' = '192.168.1.1:9200'
);

insert into SinkTable
select a, b, count(*), sum(nmuber),  window.start
from   SourceTable
group by TUMBLE(rowtime, interval  '2' minute ) , a, b;

```