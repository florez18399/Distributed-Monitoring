from flask import Blueprint, request, jsonify
from app.services.trace_service import TraceService
from app.repositories.trino_repository import TrinoRepository

trace_bp = Blueprint('trace_bp', __name__)

# Inyección de dependencias manual (podría usarse un contenedor DI en el futuro)
repository = TrinoRepository()
service = TraceService(repository)

@trace_bp.route('/api/v1/traces', methods=['GET'])
def get_traces():
    # Extraemos los filtros de la URL (ej. ?year=2026&month=2&zona=US)
    filters = {
        'year': request.args.get('year', type=int),
        'month': request.args.get('month', type=int),
        'day': request.args.get('day', type=int),
        'hour': request.args.get('hour', type=int),
        'zona': request.args.get('zona'),
        'id_transaccion': request.args.get('id_transaccion'),
        'endpoint': request.args.get('endpoint'),
        'status_response': request.args.get('status_response', type=int)
    }
    
    # Limpiamos los valores nulos
    filters = {k: v for k, v in filters.items() if v is not None}
    limit = request.args.get('limit', default=100, type=int)

    if not any(k in filters for k in ['year', 'month', 'day']):
        return jsonify({"error": "Debe proveer al menos una partición de tiempo (year, month o day) para consultar."}), 400

    try:
        data = service.get_filtered_traces(filters, limit)
        return jsonify({"data": data, "count": len(data)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@trace_bp.route('/api/v1/query', methods=['POST'])
def execute_sql():
    body = request.get_json()
    if not body or 'sql' not in body:
        return jsonify({"error": "Debe enviar un JSON con el campo 'sql'"}), 400
    
    try:
        data = service.execute_raw_sql(body['sql'])
        return jsonify({"data": data, "count": len(data)}), 200
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500