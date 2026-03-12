from gevent import monkey
monkey.patch_all()
import json
import math
from cassandra.cluster import Cluster

def cargar_json_a_cassandra():
    # 1. Conexión al contenedor de Docker
    # Usamos 127.0.0.1 porque el puerto 9042 está mapeado a tu localhost
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    # 2. Configuración inicial
    session.execute("USE tienda_keyspace;")

    # 3. Preparar la inserción (Vars Mapping)
    query = """
        INSERT INTO ventas_final (
            id_orden, fecha, estado, riesgo_tardanza, 
            cliente_nombre, cliente_ubicacion, 
            producto_nombre, producto_precio, metadatos
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)

    # 4. Leer el archivo JSON
    # Asegúrate de que la ruta sea correcta o que el archivo esté en la misma carpeta
    ruta_archivo = r'C:\Users\laura\OneDrive\Escritorio\BDNR\base_fusionada_final.json'
    
    print("Iniciando carga de datos...")
    
    with open(ruta_archivo, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
        for row in data:
            # Limpieza de precio (gestión de NaNs)
            p = row['producto']['precio']
            precio_limpio = p if (isinstance(p, (int, float)) and not math.isnan(p)) else 0.0
            
            # Ejecución del mapeo
            session.execute(prepared, (
                row['id_orden'],
                row['info_venta']['fecha'],
                row['info_venta']['estado'],
                row['info_venta']['riesgo_tardanza'],
                row['cliente']['nombre'],
                row['cliente']['ubicacion'],
                row['producto']['nombre'],
                precio_limpio,
                json.dumps(row['metadatos_enriquecidos']) # Estoc Metadata
            ))

    print(f"¡Éxito! Se han cargado {len(data)} registros.")
    cluster.shutdown()

if __name__ == "__main__":
    cargar_json_a_cassandra()