#pip install cassandra-driver

from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('gdelt')

session.execute(
    """
    INSERT INTO worldnews (country, event, total)
    VALUES (%s, %s, %s)
    """,
    ("US", "POL", 78)
)

results = session.execute("SELECT country, event, total FROM worldnews")
for result in results:
        print(result[0], result[1], result[2])
