import trino
from app.config import Config

class TrinoRepository:
    def __init__(self):
        self.conn = trino.dbapi.connect(
            host=Config.TRINO_HOST,
            port=Config.TRINO_PORT,
            user=Config.TRINO_USER,
            catalog=Config.TRINO_CATALOG,
            schema=Config.TRINO_SCHEMA,
        )

    def execute_query(self, query: str, params: tuple = None) -> list:
        cur = self.conn.cursor()
        try:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            # Formatear el resultado como una lista de diccionarios
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cur.close()