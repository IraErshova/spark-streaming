import os
from cassandra.cluster import Cluster

CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT", "9042")
CASSANDRA_KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "wiki")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")


def create_keyspace_and_table():
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()

    # Create keyspace if not exists
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)

    session.execute("""
    CREATE TABLE IF NOT EXISTS pages (
        user_id INT,
        domain TEXT,
        created_at DATE,
        page_title TEXT,
        PRIMARY KEY (user_id, page_title)
    );
    """)
    print("Keyspace and table created successfully.")
    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    create_keyspace_and_table()
