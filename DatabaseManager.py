import psycopg2

class DatabaseManager:

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    def __enter__(self):
        self.conn = psycopg2.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password,
            database = self.database
            )
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()