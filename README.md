# mhdynamo

Package mhdynamo provides a DynamoDB powered storage solution for MailHog. It
is **experimental** and currently incomplete as it does not implement the
`Search` method.

## Schema

DynamoDB is a NoSQL managed database service provided by AWS. Although it is
NoSQL you must define a minimum schema to control how data is partitioned,
sorted, and expired through TTL settings. This package takes an opinionated
view of how those settings must be configured.

| Attribute     | Name          | Type   |
|---------------|---------------|--------|
| Partition Key | `CreatedDate` | String |
| Sort Key      | `ID`          | String |
| TTL Attribute | `Expires`     | Number |

## Partitioning

DynamoDB stores all data with the same Partition Key together. It works best if
the data can be spread out among partitions in a relatively uniform pattern.

The initial implementation of this package used the unique ID of each message
as a partition key for optimal distribution. The problem with this approach is
application code must **already** know the partition key for many operations
such as listing records in a particular order. Having a predictable partition
key makes those operations easier but if all items had the **same** key it
would degrade performance.

A different approach was taken that uses the created date (but not time) of
each message for partitioning. This allows for predictable partition keys while
still having some level of partition distribution. This is consistent with the
[AWS Recommendations For Time-Series Data](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-time-series.html)

It is still possible for partitions to be uneven if there is a spike in mail on
a particular day. If more granularity is desired this can be reasonably
adjusted such as narrowing to half days (am/pm) or to the creation hour.

In this scheme table items look like this:

| CreatedDate    | ID                                      | Expires    | Msg              |
|----------------|-----------------------------------------|------------|------------------|
| `"2019-07-31"` | `"1564572176345666420\|msg9@localhost"` | 1565176976 | `{message data}` |
| `"2019-07-31"` | `"1564567856345654075\|msg8@localhost"` | 1565172656 | `{message data}` |
| `"2019-07-31"` | `"1564563536345641730\|msg7@localhost"` | 1565168336 | `{message data}` |
|                |                                         |            |                  |
| `"2019-07-30"` | `"1564472816345629385\|msg6@localhost"` | 1565077616 | `{message data}` |
| `"2019-07-30"` | `"1564468496345617040\|msg5@localhost"` | 1565073296 | `{message data}` |
| `"2019-07-30"` | `"1564464176345604695\|msg4@localhost"` | 1565068976 | `{message data}` |
|                |                                         |            |                  |
| `"2019-07-29"` | `"1564373456345592350\|msg3@localhost"` | 1564978256 | `{message data}` |
| `"2019-07-29"` | `"1564369136345580005\|msg2@localhost"` | 1564973936 | `{message data}` |
| `"2019-07-29"` | `"1564364816345567660\|msg1@localhost"` | 1564969616 | `{message data}` |

The partition keys can be predicted by querying for items from the current day
and then working backwards until we hit the extent of our TTL setting.

## Message IDs

MailHog messages have a unique ID that looks like `random@host`. Because of our
partitioning scheme and sorting scheme defined above, just knowing
`random@host` is not sufficient to identify a record. Instead the creation time
(in nanoseconds) is combined with the ID to generate a new ID that is sortable
and can be decoded to find the appropriate partition key. This means an ID like
`"1564364816345567660|msg1@localhost"` originally has the id `msg1@localhost`
and was created at 1564364816345567660 so it is in the `"2019-07-29"` partition.

## Improvements and Limitations

This backend must implement the mailhog Storage interface which does not
provide `context.Context` for timeouts or cancellation which I would like. If
MailHog were extended to provide those field then this package could be
modified to respect those deadlines.
