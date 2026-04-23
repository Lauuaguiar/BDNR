from gevent import monkey
monkey.patch_all()

import json
from cassandra.cluster import Cluster
from datetime import datetime

def parse_ts(ts):
    if ts:
        return datetime.fromisoformat(ts)
    return None

def cargar_json_a_cassandra():
    # 1. Conexión
    cluster = Cluster(['127.0.0.1'], port=9042)
    session = cluster.connect()

    # 2. Keyspace
    session.execute("USE tienda_keyspace;")  # ⚠️ cambia si usas otro

    # 3. Query adaptada a la NUEVA tabla
    query = """
        INSERT INTO pedidos_dashboard (
            id_orden, fecha, estado, riesgo_tardanza,
            cliente_nombre, cliente_ubicacion,
            producto_nombre, precio,
            market_event_time_1, market_category_1, market_brand_1, market_price_1,
            market_event_time_2, market_category_2, market_brand_2, market_price_2
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    prepared = session.prepare(query)

    # 4. Ruta JSON limpio
    ruta_archivo = "datos_limpios.json"

    print("Iniciando carga de datos...")

    with open(ruta_archivo, 'r', encoding='utf-8') as f:
        data = json.load(f)

        for row in data:
            session.execute(prepared, (
                row["id_orden"],
                parse_ts(row["fecha"]),
                row["estado"],
                row["riesgo_tardanza"],
                row["cliente_nombre"],
                row["cliente_ubicacion"],
                row["producto_nombre"],
                row["precio"],

                parse_ts(row.get("market_event_time_1")),
                row.get("market_category_1"),
                row.get("market_brand_1"),
                row.get("market_price_1"),

                parse_ts(row.get("market_event_time_2")),
                row.get("market_category_2"),
                row.get("market_brand_2"),
                row.get("market_price_2")
            ))

    print(f"¡Éxito! Se han cargado {len(data)} registros.")
    cluster.shutdown()

if __name__ == "__main__":
    cargar_json_a_cassandra()
