from flask import Flask, request, jsonify
import requests
import json

app = Flask(__name__)

# Configuración de Backendless
BACKENDLESS_URL = "https://api.backendless.com"
APP_ID = "1B581BD2-5E1E-4210-9C5E-F957BAAA8D23"
API_KEY = "C2A5409D-5DA6-42E3-B71A-D3D8201A8FC1"
TABLE_NAME = "logs"

# Headers para las peticiones a Backendless
headers = {
    "Content-Type": "application/json",
    "application-id": APP_ID,
    "secret-key": API_KEY
}

def get_base_url():
    """Construye la URL base para operaciones de datos"""
    return f"{BACKENDLESS_URL}/{APP_ID}/{API_KEY}/data/{TABLE_NAME}"


# CREATE - Crear un nuevo log
@app.route('/logs', methods=['POST'])
def create_log():
    """
    Crear un nuevo registro en la tabla logs
    Espera JSON: {"document_id": int, "transaction_id": int, "user_id": int}
    """
    try:
        data = request.get_json()
        
        # Validar que se proporcionen todos los campos requeridos
        if not data:
            return jsonify({"error": "No se proporcionaron datos"}), 400
            
        required_fields = ["document_id", "transaction_id", "user_id"]
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Campo requerido faltante: {field}"}), 400
            
            # Validar que sean enteros
            try:
                data[field] = int(data[field])
            except (ValueError, TypeError):
                return jsonify({"error": f"El campo {field} debe ser un entero"}), 400
        
        # Crear el objeto para enviar a Backendless
        log_data = {
            "document_id": data["document_id"],
            "transaction_id": data["transaction_id"],
            "user_id": data["user_id"]
        }
        
        # Realizar la petición POST a Backendless
        url = get_base_url()
        response = requests.post(url, headers=headers, json=log_data)
        
        if response.status_code == 200 or response.status_code == 201:
            return jsonify(response.json()), 201
        else:
            return jsonify({
                "error": "Error al crear el log en Backendless",
                "details": response.text
            }), response.status_code
            
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

# READ - Obtener todos los logs
@app.route('/logs', methods=['GET'])
def get_all_logs():
    """
    Obtener todos los registros de la tabla logs
    Parámetros opcionales: pageSize, offset, sortBy
    """
    try:
        # Obtener parámetros de consulta opcionales
        page_size = request.args.get('pageSize', 100)
        offset = request.args.get('offset', 0)
        sort_by = request.args.get('sortBy', 'created')
        
        # Construir URL con parámetros
        url = f"{get_base_url()}?pageSize={page_size}&offset={offset}&sortBy={sort_by}"
        
        # Realizar la petición GET a Backendless
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({
                "error": "Error al obtener los logs de Backendless",
                "details": response.text
            }), response.status_code
            
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500


# READ - Obtener un log individual por transaction_id
@app.route('/logs/by-transaction', methods=['GET'])
def get_log_by_transaction_id():
    """
    Obtener un registro individual por su transaction_id usando ?transaction_id= en la query
    """
    transaction_id = request.args.get('transaction_id')
    if not transaction_id:
        return jsonify({"error": "Se requiere el parámetro transaction_id"}), 400
    try:
        where_clause = f"transaction_id = {transaction_id}"
        url = f"{get_base_url()}?where={where_clause}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                return jsonify(data[0]), 200
            elif isinstance(data, dict) and data.get('transaction_id') is not None:
                return jsonify(data), 200
            else:
                return jsonify({"error": "Log no encontrado"}), 404
        else:
            return jsonify({
                "error": "Error al obtener el log de Backendless",
                "details": response.text
            }), response.status_code
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

# UPDATE - Actualizar user_id por transaction_id
@app.route('/logs/by-transaction', methods=['PUT'])
def update_user_id_by_transaction_id():
    """
    Actualizar el user_id de un log usando transaction_id como query param
    Espera JSON: {"user_id": nuevo_valor}
    """
    transaction_id = request.args.get('transaction_id')
    if not transaction_id:
        return jsonify({"error": "Se requiere el parámetro transaction_id"}), 400
    try:
        # Buscar el log por transaction_id
        where_clause = f"transaction_id = {transaction_id}"
        url = f"{get_base_url()}?where={where_clause}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            return jsonify({"error": "Error al buscar el log", "details": response.text}), response.status_code
        data = response.json()
        if isinstance(data, list) and len(data) > 0:
            log = data[0]
        elif isinstance(data, dict) and data.get('transaction_id') is not None:
            log = data
        else:
            return jsonify({"error": "Log no encontrado"}), 404
        object_id = log.get('objectId')
        if not object_id:
            return jsonify({"error": "No se encontró objectId para el log"}), 404
        # Obtener el nuevo user_id
        req_data = request.get_json()
        if not req_data or 'user_id' not in req_data:
            return jsonify({"error": "Se requiere el campo user_id en el body"}), 400
        try:
            new_user_id = int(req_data['user_id'])
        except (ValueError, TypeError):
            return jsonify({"error": "user_id debe ser un entero"}), 400
        # Actualizar el log
        update_url = f"{get_base_url()}/{object_id}"
        update_data = {"user_id": new_user_id}
        update_response = requests.put(update_url, headers=headers, json=update_data)
        if update_response.status_code == 200:
            return jsonify(update_response.json()), 200
        else:
            return jsonify({"error": "Error al actualizar el log", "details": update_response.text}), update_response.status_code
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

# READ - Buscar logs por criterios específicos
@app.route('/logs/search', methods=['GET'])
def search_logs():
    """
    Buscar logs por criterios específicos
    Parámetros: document_id, transaction_id, user_id (opcionales)
    """
    try:
        # Construir la consulta WHERE basada en los parámetros
        where_conditions = []
        
        if request.args.get('document_id'):
            where_conditions.append(f"document_id = {request.args.get('document_id')}")
        if request.args.get('transaction_id'):
            where_conditions.append(f"transaction_id = {request.args.get('transaction_id')}")
        if request.args.get('user_id'):
            where_conditions.append(f"user_id = {request.args.get('user_id')}")
        
        url = get_base_url()
        if where_conditions:
            where_clause = " AND ".join(where_conditions)
            url += f"?where={where_clause}"
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        else:
            return jsonify({
                "error": "Error al buscar logs en Backendless",
                "details": response.text
            }), response.status_code
            
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

# UPDATE - Actualizar un log
@app.route('/logs/<string:log_id>', methods=['PUT'])
def update_log(log_id):
    """
    Actualizar un registro específico por su objectId
    Espera JSON con los campos a actualizar
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({"error": "No se proporcionaron datos para actualizar"}), 400
        
        # Validar y convertir campos si están presentes
        update_data = {}
        allowed_fields = ["document_id", "transaction_id", "user_id"]
        
        for field in allowed_fields:
            if field in data:
                try:
                    update_data[field] = int(data[field])
                except (ValueError, TypeError):
                    return jsonify({"error": f"El campo {field} debe ser un entero"}), 400
        
        if not update_data:
            return jsonify({"error": "No se proporcionaron campos válidos para actualizar"}), 400
        
        url = f"{get_base_url()}/{log_id}"
        response = requests.put(url, headers=headers, json=update_data)
        
        if response.status_code == 200:
            return jsonify(response.json()), 200
        elif response.status_code == 404:
            return jsonify({"error": "Log no encontrado"}), 404
        else:
            return jsonify({
                "error": "Error al actualizar el log en Backendless",
                "details": response.text
            }), response.status_code
            
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

# DELETE - Eliminar un log
@app.route('/logs/<string:log_id>', methods=['DELETE'])
def delete_log(log_id):
    """
    Eliminar un registro específico por su objectId
    """
    try:
        url = f"{get_base_url()}/{log_id}"
        response = requests.delete(url, headers=headers)
        
        if response.status_code == 200:
            return jsonify({"message": "Log eliminado exitosamente"}), 200
        elif response.status_code == 404:
            return jsonify({"error": "Log no encontrado"}), 404
        else:
            return jsonify({
                "error": "Error al eliminar el log en Backendless",
                "details": response.text
            }), response.status_code
            
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

# DELETE - Eliminar múltiples logs por user_id
@app.route('/logs/delete', methods=['DELETE'])
def bulk_delete_logs():
    """
    Eliminar múltiples logs por user_id
    Parámetro requerido: ?user_id=123
    """
    try:
        user_id = request.args.get('user_id')
        
        if not user_id:
            return jsonify({
                "error": "Se requiere el parámetro 'user_id' en la URL",
                "example": "?user_id=123"
            }), 400
        
        # Validar que user_id sea un número
        try:
            user_id_int = int(user_id)
        except ValueError:
            return jsonify({"error": "user_id debe ser un número entero"}), 400
        
        print(f"DEBUG: Eliminando logs con user_id = {user_id_int}")
        
        # Primero buscar todos los logs con este user_id para eliminarlos uno por uno
        where_clause = f"user_id = {user_id_int}"
        search_url = f"{get_base_url()}?where={where_clause}"
        search_response = requests.get(search_url, headers=headers)
        
        if search_response.status_code != 200:
            return jsonify({
                "error": "Error al buscar logs",
                "details": search_response.text
            }), search_response.status_code
        
        logs_data = search_response.json()
        if not logs_data or len(logs_data) == 0:
            return jsonify({
                "message": "No se encontraron logs con ese user_id",
                "deleted_count": 0
            }), 200
        
        print(f"DEBUG: Se encontraron {len(logs_data)} logs para eliminar")
        
        # Eliminar cada log individualmente
        deleted_count = 0
        errors = []
        
        for log in logs_data:
            object_id = log.get('objectId')
            if object_id:
                delete_url = f"{get_base_url()}/{object_id}"
                delete_response = requests.delete(delete_url, headers=headers)
                
                if delete_response.status_code == 200:
                    deleted_count += 1
                    print(f"DEBUG: Eliminado log {object_id}")
                else:
                    errors.append(f"Error eliminando {object_id}: {delete_response.text}")
        
        if errors:
            return jsonify({
                "message": f"Se eliminaron {deleted_count} logs, pero hubo errores",
                "deleted_count": deleted_count,
                "errors": errors
            }), 207  # Multi-Status
        else:
            return jsonify({
                "message": "Logs eliminados exitosamente",
                "deleted_count": deleted_count
            }), 200
            
    except Exception as e:
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint no encontrado"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Error interno del servidor"}), 500

if __name__ == '__main__':
    print("🚀 Iniciando servicio CRUD para tabla 'logs'")
    print("📊 Endpoints disponibles:")
    print("   GET  /logs                     - Obtener todos los logs")
    print("   GET  /logs/<id>                - Obtener log por ID")
    print("   GET  /logs/search              - Buscar logs por criterios")
    print("   POST /logs                     - Crear nuevo log")
    print("   PUT  /logs/<id>                - Actualizar log")
    print("   DELETE /logs/<id>              - Eliminar log")
    print("   DELETE /logs/delete?user_id=X  - Eliminar logs por user_id")
    print("🔗 Base URL: http://localhost:8080")
    app.run(debug=True, host='0.0.0.0', port=8080)
