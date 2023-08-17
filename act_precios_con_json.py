import pymysql
import json
import schedule
import time
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

ZAFIRO_HOST = os.getenv("ZAFIRO_HOST")
ZAFIRO_USER = os.getenv("ZAFIRO_USER")
ZAFIRO_PASSWORD = os.getenv("ZAFIRO_PASSWORD")
ZAFIRO_DATABASE = os.getenv("ZAFIRO_DATABASE")

SISCON_HOST = os.getenv("SISCON_HOST")
SISCON_USER = os.getenv("SISCON_USER")
SISCON_PASSWORD = os.getenv("SISCON_PASSWORD")
SISCON_DATABASE = os.getenv("SISCON_DATABASE")

def conectar_db(HOST, USER, PASSWORD, DATABASE):
    try:
        conn = pymysql.connect(
            host = HOST,
            user = USER,
            password = PASSWORD,
            database = DATABASE
        )
        return conn
    except pymysql.Error as e:
        print(f"\nError al conectarse a {DATABASE} \n\n",e)
        raise

def verificar_y_actualizar_precios():
    try:
        # Registrar el tiempo de inicio
        tiempo_inicio = time.time()
        tiempo_inicio_legible = time.strftime('%H:%M:%S', time.localtime(tiempo_inicio))
        
        # Cargar el archivo JSON con los precios actualizados en un diccionario
        with open("precios_actualizados.json", "r") as json_file:
            precios_actualizados = json.load(json_file)

        # Configuración de la conexión a la base de datos siscon
        siscon_conn = conectar_db(SISCON_HOST, SISCON_USER, SISCON_PASSWORD, SISCON_DATABASE)
        siscon_cursor = siscon_conn.cursor()

        # Consultar la tabla "articulosZafiro" en "siscon" y comparar con los precios del JSON
        siscon_cursor.execute("SELECT id_articulo, pcio_com_siva, pcio_vta_siva FROM articulosZafiro")
        resultados_siscon = siscon_cursor.fetchall()

        actualizaciones = 0
        for id_articulo, pcio_com_siva, pcio_vta_siva in resultados_siscon:
            if id_articulo in precios_actualizados:
                precio_actualizado = precios_actualizados[id_articulo]
                if precio_actualizado["pcio_com_siva"] != pcio_com_siva or precio_actualizado["pcio_vta_siva"] != pcio_vta_siva:
                    # Realizar la actualización en la base de datos siscon
                    update_query = "UPDATE articulosZafiro SET pcio_com_siva = %s, pcio_vta_siva = %s WHERE id_articulo = %s"
                    siscon_cursor.execute(update_query, (precio_actualizado["pcio_com_siva"], precio_actualizado["pcio_vta_siva"], id_articulo))
                    siscon_conn.commit()
                    actualizaciones += 1
                    print(f"articulo {id_articulo} fue actualizado")
        
        tiempo_final = time.time()
        tiempo_final_legible = time.strftime('%H:%M:%S', time.localtime(tiempo_final))
        tiempo_transcurrido = tiempo_final - tiempo_inicio
        
        # Calcular las horas, minutos y segundos
        horas = int(tiempo_transcurrido // 3600)
        minutos = int((tiempo_transcurrido % 3600) // 60)
        segundos = int(tiempo_transcurrido % 60)
        # Formatear el tiempo transcurrido
        tiempo_transcurrido_legible = f"{horas:02}:{minutos:02}:{segundos:02}"
        
        print(f"\nActualizaciones realizadas: {actualizaciones}")
        print(f"Tiempo de actualización: {tiempo_transcurrido_legible}\nHora de inicio: {tiempo_inicio_legible}\nHora de finalización: {tiempo_final_legible}")
    
    finally:
        siscon_cursor.close()
        siscon_conn.close()


def generar_json_precios_actualizados():
    try:
        # Configuración de la conexión a la base de datos zafiro_dro
        zafiro_conn = None
        zafiro_cursor = None
        zafiro_conn = conectar_db(ZAFIRO_HOST, ZAFIRO_USER, ZAFIRO_PASSWORD, ZAFIRO_DATABASE)
        zafiro_cursor = zafiro_conn.cursor()

        # Consulta para obtener los precios actualizados
        zafiro_cursor.execute("SELECT id_articulo, pcio_com_siva, pcio_vta_siva FROM articulos")
        resultados = zafiro_cursor.fetchall()

        # Crear un diccionario con los precios actualizados
        precios_actualizados = {}
        for id_articulo, pcio_com_siva, pcio_vta_siva in resultados:
            precios_actualizados[id_articulo] = {"pcio_com_siva": pcio_com_siva, "pcio_vta_siva": pcio_vta_siva}

        # Guardar el diccionario en un archivo JSON
        with open("precios_actualizados.json", "w") as json_file:
            json.dump(precios_actualizados, json_file)
    
        print("\nJSON creado\n")
    finally:
        if zafiro_cursor:
            zafiro_cursor.close()
        if zafiro_conn:
            zafiro_conn.close()

    verificar_y_actualizar_precios()
    
schedule.every().day.at("18:00").do(generar_json_precios_actualizados)
schedule.every().day.at("06:00").do(generar_json_precios_actualizados)

#primero ejecuto la función y luego temporizo
# generar_json_precios_actualizados()
# schedule.every(12).hours.do(generar_json_precios_actualizados)

#cada 15 segundos para pruebas
#schedule.every(15).seconds.do(generar_json_precios_actualizados)

while True:    
    schedule.run_pending()
    time.sleep(1)