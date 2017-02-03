from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS gdelt WITH REPLICATION = {
        'class' : 'SimpleStrategy',
        'replication_factor' : 3
    }
    """
)

session.set_keyspace('gdelt')

session.execute(
    """
    CREATE TABLE worldnews (
        country text PRIMARY KEY,
        event text,
        total int
    );
    """
)
