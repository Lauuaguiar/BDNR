from gevent import monkey
monkey.patch_all()

import json
from cassandra.cluster import Cluster

def realizar_analisis():
    try:
        # Conexión al clúster (localhost puerto 9042)
        cluster = Cluster(['127.0.0.1'], port=9042)
        session = cluster.connect('tienda_keyspace')

        print(f"\n{'ID ORDEN':<10} | {'PRODUCTO':<25} | {'NUESTRO P.':<10} | {'MERCADO (KZ)':<12}")
        print("-" * 65)

        # Consultamos 10 registros para ver la comparativa
        rows = session.execute("SELECT id_orden, producto_nombre, producto_precio, metadatos FROM ventas_final LIMIT 10")

        for row in rows:
            # Procesamos el JSON de los metadatos enriquecidos
            meta = json.loads(row.metadatos)
            mercado = meta.get('contexto_mercado_kz', [])
            
            # Obtenemos el precio de la competencia si existe
            precio_comp = mercado[0].get('price', 'N/A') if mercado else "Sin datos"
            
            print(f"{row.id_orden:<10} | {row.producto_nombre[:25]:<25} | {row.producto_precio:<10.2f} | {precio_comp}")

        cluster.shutdown()
        
    except Exception as e:
        print(f"Error detectado: {e}")

if __name__ == "__main__":
    realizar_analisis()