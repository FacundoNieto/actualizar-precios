import pymysql
import json
import schedule
import time
from datetime import datetime, timedelta
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
    conectado = False
    while not conectado:
        try:
            conn = pymysql.connect(
                host = HOST,
                user = USER,
                password = PASSWORD,
                database = DATABASE
            )
            conectado = True
            return conn
        except pymysql.OperationalError as e:
            print(f"\nError de conexión a {DATABASE}\n{e}")
            print(f"Reintentando conexión a {DATABASE}")
            hora = time.time()
            hora_legible = time.strftime('%H:%M:%S %d-%m-%Y', time.localtime(hora))
            print(f"Hora: {hora_legible}\n")
            time.sleep(5)

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
        tiempo_final_legible = time.strftime('%H:%M:%S %d-%m-%Y', time.localtime(tiempo_final))
        tiempo_transcurrido = tiempo_final - tiempo_inicio
        
        # Calcular las horas, minutos y segundos
        horas = int(tiempo_transcurrido // 3600)
        minutos = int((tiempo_transcurrido % 3600) // 60)
        segundos = int(tiempo_transcurrido % 60)
        # Formatear el tiempo transcurrido
        tiempo_transcurrido_legible = f"{horas:02}:{minutos:02}:{segundos:02}"
        
        print(f"\nActualizaciones realizadas: {actualizaciones}")
        print(f"Tiempo de actualización: {tiempo_transcurrido_legible}\nHora de inicio: {tiempo_inicio_legible}\nHora de finalización: {tiempo_final_legible}")
        print("Fecha y hora:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '\n')
    
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
    
def determinar_meses(postdtd):
    if postdtd == '5':
        meses = 1
    elif postdtd == '6':
        meses = 2
    elif postdtd == '7':
        meses = 3
    return meses

def verificar_postdatada():
        siscon_conn = conectar_db(SISCON_HOST, SISCON_USER, SISCON_PASSWORD, SISCON_DATABASE)
        siscon_cursor = siscon_conn.cursor()

        try:
            sql_select = 'SELECT id, created_at, fecha_receta, fecha_vencimiento, postdatada, estado_solicitud_id, ant_postdatada, renovaciones FROM pedido_medicamento'
            siscon_cursor.execute(sql_select)
            result = siscon_cursor.fetchall() # ((),(),(),(),())

            actualizados = 0
            for id, created_at, fecha_receta, fecha_vencimiento, postdatada, estado_solicitud_id, ant_postdatada, renovaciones in result:
                #obtengo la fecha de vencimiento en segundos para comparar con la hora actual
                fecha_vencimiento_datetime = datetime.combine(fecha_vencimiento, datetime.min.time()) # queda yyyy-mm-dd 00:00:00
                fecha_vencimiento_en_segundos = fecha_vencimiento_datetime.timestamp() #float

                hora_actual = time.time() #float
                mes_actual = time.strftime('%m', time.localtime(hora_actual)) #string con el número del mes

                if hora_actual >= fecha_vencimiento_en_segundos:
                    #Consulto el valor de postdatada: 5 = 1 mes, 6 = 2 meses, 7 = 3 meses
                    if postdatada in ('5','6','7'):
                        sql_update = 'UPDATE pedido_medicamento SET postdatada = 8 WHERE id = %s'
                        siscon_cursor.execute(sql_update, id)
                        siscon_conn.commit()
                        meses = determinar_meses(postdatada)        
                    elif postdatada == '8':
                        meses = determinar_meses(ant_postdatada)
                    else:
                        print(f'pedido {id} tiene una postdatada distinta a 5 6 7 y 8')
                        continue
                    
                    new_mes_vencimiento = int(mes_actual) + meses 
                    if new_mes_vencimiento > 12: # puede dar 13, 14 ó 15
                        new_year = fecha_vencimiento.year + 1
                        new_month = new_mes_vencimiento - 12 # será 1, 2 ó 3
                        new_day = min(fecha_vencimiento.day,  (datetime(new_year, new_month + 1 , 1) - timedelta(days=1)).day)
                        new_fecha_vencimiento = fecha_vencimiento.replace(year = new_year, month = new_month, day = new_day) 
                    else:
                        mes_siguiente = new_mes_vencimiento + 1
                        if mes_siguiente > 12:
                            mes_siguiente = mes_siguiente - 12
                        new_day = min(fecha_vencimiento.day,  (datetime(fecha_vencimiento.year, mes_siguiente, 1) - timedelta(days=1)).day)
                        new_fecha_vencimiento = fecha_vencimiento.replace(month = new_mes_vencimiento, day = new_day)

                    #Actualizar fecha de vencimiento
                    sql_update_fch_ven = 'UPDATE pedido_medicamento SET fecha_vencimiento = %s WHERE id = %s'
                    values_fch_ven = (new_fecha_vencimiento,id)
                    siscon_cursor.execute(sql_update_fch_ven, values_fch_ven)
                    siscon_conn.commit()

                    # Actualizar created_at
                    mes_siguiente = int(mes_actual) + 1
                    if mes_siguiente > 12:
                        mes_siguiente = mes_siguiente - 12
                    new_day = min(created_at.day, (datetime(created_at.year, mes_siguiente, 1) - timedelta(days=1)).day)
                    # se cambia el año sólo cuando el mes actual es menor al mes de created_at
                    if int(mes_actual) >= created_at.month:
                        new_created_at = created_at.replace(month = int(mes_actual), day = new_day)
                    else:
                        new_created_at = created_at.replace(year = created_at.year + 1, month = int(mes_actual), day = new_day)
                    # si la nueva created_at supera a la fecha actual, entonces se deja la fecha actual como nueva created_at
                    if new_created_at.timestamp() > hora_actual: #comparo float con float
                        hoy = datetime.now()
                        new_created_at = new_created_at.replace(year = hoy.year, month = hoy.month, day = hoy.day)

                    sql_update_cr_at = 'UPDATE pedido_medicamento SET created_at = %s WHERE id = %s'
                    values_cr_at = (new_created_at, id)
                    siscon_cursor.execute(sql_update_cr_at, values_cr_at)
                    siscon_conn.commit()

                    # Actualizar fecha de receta
                    new_fecha_receta = new_created_at.date()
                    sql_update_fch_rec = 'UPDATE pedido_medicamento SET fecha_receta = %s WHERE id = %s'
                    values_fch_rec = (new_fecha_receta,id)
                    siscon_cursor.execute(sql_update_fch_rec, values_fch_rec)
                    siscon_conn.commit()

                    # Actualizar estado_solicitud_id, ahora será 4 (AUTORIZADA)
                    sql_update_estado = 'UPDATE pedido_medicamento SET estado_solicitud_id = 4 WHERE id = %s'
                    siscon_cursor.execute(sql_update_estado, id)
                    siscon_conn.commit()

                    # Actualizar ant_postdatada, ant_est_sol y renovaciones
                    if renovaciones is None or renovaciones == '0':
                        renovaciones = 1
                        sql_upd_info = 'UPDATE pedido_medicamento SET ant_postdatada = %s, ant_est_sol = %s, renovaciones = %s WHERE id = %s'
                        values_info = (postdatada, estado_solicitud_id, renovaciones, id)
                        siscon_cursor.execute(sql_upd_info, values_info)
                        siscon_conn.commit()

                    else:
                        renovaciones = int(renovaciones) + 1
                        sql_upd_renovaciones = 'UPDATE pedido_medicamento SET renovaciones = %s WHERE id = %s'
                        values_renovaciones = (renovaciones, id)
                        siscon_cursor.execute(sql_upd_renovaciones, values_renovaciones)
                        siscon_conn.commit()
                    
                    print(f'Pedido {id} actualizado')
                    actualizados = actualizados + 1 

        finally:
            siscon_cursor.close()
            siscon_conn.close()
            print('\nPedidos actualizados:', actualizados)
            print("Fecha y hora:", datetime.now().strftime("%d/%m/%Y %H:%M:%S"), '\n')

schedule.every().day.at("18:00").do(generar_json_precios_actualizados)
schedule.every().day.at("06:00").do(generar_json_precios_actualizados)
schedule.every().day.at("01:00").do(verificar_postdatada)

#primero ejecuto la función y luego temporizo
# generar_json_precios_actualizados()
# schedule.every(12).hours.do(generar_json_precios_actualizados)

#cada 15 segundos para pruebas
#schedule.every(15).seconds.do(generar_json_precios_actualizados)

while True:    
    schedule.run_pending()
    time.sleep(1)
