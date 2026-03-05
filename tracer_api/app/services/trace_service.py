from app.repositories.trino_repository import TrinoRepository
from app.config import Config

class TraceService:
    def __init__(self, repository: TrinoRepository):
        self.repository = repository
        self.table = Config.TRACES_TABLE

    def get_filtered_traces(self, filters: dict, limit: int = 100) -> list:
        # Construcción dinámica y segura de la consulta
        query = f"SELECT * FROM {self.table} WHERE 1=1"
        params = []

        # Campos permitidos para filtrar (particiones y claves principales)
        allowed_filters = ['year', 'month', 'day', 'hour', 'zona', 'id_transaccion', 'endpoint', 'status_response']
        
        for key, value in filters.items():
            if key in allowed_filters and value is not None:
                query += f" AND {key} = ?"
                params.append(value)

        query += f" LIMIT {limit}"
        
        return self.repository.execute_query(query, tuple(params))

    def execute_raw_sql(self, sql_query: str) -> list:
        # Aquí se podrían agregar validaciones de seguridad básicas (ej. prohibir DROP, DELETE)
        if not sql_query.strip().upper().startswith("SELECT"):
            raise ValueError("Solo se permiten consultas SELECT.")
        
        return self.repository.execute_query(sql_query)