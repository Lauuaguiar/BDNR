from gevent import monkey
monkey.patch_all()
from cassandra.cluster import Cluster

def gestionar_prioridad():
    try:
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect('tienda_keyspace')

        # 1. Buscamos los IDs que tienen riesgo en Caguas
        print("Buscando pedidos con riesgo en Caguas...")
        rows = session.execute("SELECT id_orden FROM ventas_final WHERE cliente_ubicacion = 'Caguas' AND riesgo_tardanza = true ALLOW FILTERING")
        ids = [row.id_orden for row in rows]

        if not ids:
            print("No se encontraron pedidos con riesgo.")
            return

        # 2. Actualizamos esos IDs a PRIORITY_SHIPMENT
        # Convertimos la lista de Python a un formato que Cassandra entienda: (ID1, ID2, ...)
        id_list_str = ", ".join(map(str, ids))
        query = f"UPDATE ventas_final SET estado = 'PRIORITY_SHIPMENT' WHERE id_orden IN ({id_list_str})"
        
        session.execute(query)
        print(f"¡Éxito! Se han actualizado {len(ids)} pedidos a PRIORITY_SHIPMENT.")

        cluster.shutdown()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    gestionar_prioridad()