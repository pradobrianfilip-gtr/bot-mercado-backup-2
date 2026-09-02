"""
Bot de monitoreo de noticias del mercado de EEUU -> alertas a Discord
=====================================================================

Que hace:
  - Consulta periodicamente varias fuentes gratuitas:
      * FRED (Reserva Federal de St. Louis)   -> tipos de interes, desempleo
      * EIA (Energy Information Administration) -> precio del petroleo (WTI)
      * GDELT (proyecto abierto de eventos globales) -> geopolitica / conflictos
      * NewsAPI (opcional, si tienes API key)  -> titulares generales (Fed, dolar...)
      * Alpaca Market Data (gratis, cuenta paper) -> volumen y spread bid-ask del SPY,
        maximo/minimo semanal del dolar (UUP), y el ETF GLD como segunda
        referencia del oro
      * xaus.com (gratis, sin llave) -> precio real del oro XAU/USD y XAU/EUR
        (fuente principal). El reporte periodico del oro combina AMBAS fuentes
        (xaus.com + GLD de Alpaca) en el mismo mensaje de Discord, para tener
        dos referencias y que si una falla la otra siga funcionando.
  - Filtra por palabras clave relevantes.
  - Traduce automaticamente al espanol los titulares que llegan en ingles
    (GDELT y NewsAPI), para que todo el mensaje en Discord quede en espanol.
  - Publica en tu canal de Discord via webhook.
  - Convierte y muestra todas las horas en Europe/Madrid, para que
    identifiques facilmente el solapamiento Londres-Nueva York (~14:00-17:30
    hora de Madrid, 15:30 en verano segun el cambio de horario de EEUU).

IMPORTANTE sobre liquidez: las noticias NO miden liquidez directamente (eso es
volumen, profundidad de mercado y spread bid-ask, que vive en los datos de
mercado, no en el texto de un articulo). Lo que hace este bot es: (1) vigilar
el volumen y el spread del SPY en tiempo casi real, (2) avisar cuando se
salen de lo normal, y (3) decirte si eso coincidio con una noticia reciente
o si parece un movimiento sin causa aparente en las noticias que capturamos.

Requisitos:
    pip install requests apscheduler deep-translator

Configuracion minima necesaria (rellena mas abajo o usa variables de entorno):
    DISCORD_WEBHOOK_URL  -> Discord: Ajustes del canal > Integraciones > Webhooks
    FRED_API_KEY         -> gratis en https://fred.stlouisfed.org/docs/api/api_key.html
    EIA_API_KEY          -> gratis en https://www.eia.gov/opendata/register.php
    NEWSAPI_KEY          -> opcional, gratis (limitado) en https://newsapi.org
    ALPACA_API_KEY / ALPACA_SECRET_KEY -> gratis con una cuenta paper en
                             https://alpaca.markets (da acceso a datos de mercado
                             en tiempo casi real via el feed IEX, sin necesidad
                             de depositar dinero real)

Sistema PRIMARIO / RESPALDO (opcional, para redundancia):
    ROL_BOT = "primario" (por defecto) o "backup"
    URL_BOT_PRIMARIO = solo si ROL_BOT="backup" -- la URL publica del bot
                        primario (ej. https://bot-mercado-50r4.onrender.com)
    El bot en modo "backup" corre exactamente el mismo codigo, pero se queda
    en silencio (no manda nada a Discord) mientras el primario responda bien.
    Si el primario deja de responder, el backup manda un aviso de "tomo el
    relevo" y empieza a enviar las alertas el solo, hasta que el primario
    vuelva -- para no duplicar mensajes en circunstancias normales.

Este script es un PUNTO DE PARTIDA funcional, no un producto terminado:
revisa los umbrales, palabras clave e intervalos segun tus necesidades.

Nota sobre hosting gratis (ej. Render): estos servicios gratuitos necesitan
que el programa "escuche" en un puerto web para saber que sigue vivo. Por eso
este script levanta un mini servidor (mas abajo, iniciar_servidor_web) que
solo responde "Bot activo" -- no hace falta tocarlo, ya viene listo.
"""

import os
import time
import statistics
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from collections import deque
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from apscheduler.schedulers.blocking import BlockingScheduler
from deep_translator import GoogleTranslator

# ---------------------------------------------------------------------------
# CONFIGURACION
# ---------------------------------------------------------------------------

DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "PON_AQUI_TU_WEBHOOK")
FRED_API_KEY = os.environ.get("FRED_API_KEY", "PON_AQUI_TU_API_KEY")
EIA_API_KEY = os.environ.get("EIA_API_KEY", "PON_AQUI_TU_API_KEY")
NEWSAPI_KEY = os.environ.get("NEWSAPI_KEY", "")  # opcional
ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "PON_AQUI_TU_API_KEY")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "PON_AQUI_TU_SECRET_KEY")

# Simbolo que usamos como proxy de liquidez del mercado de EEUU en general.
# SPY = ETF del S&P 500, uno de los mas liquidos que existen; sirve de referencia.
SIMBOLO_LIQUIDEZ = "SPY"

# Simbolo que usamos como proxy del dolar (UUP = ETF que replica el indice dolar DXY)
SIMBOLO_DOLAR = "UUP"
UMBRAL_CAIDA_DOLAR = 1.0   # % de caida desde el maximo de la semana para avisar
UMBRAL_SUBIDA_DOLAR = 0.1  # % que debe superar el maximo previo para considerarlo ruptura

# Simbolo que usamos como referencia adicional del oro en Alpaca (ETF que
# sigue al oro; se muestra junto al precio real de xaus.com como segunda fuente)
SIMBOLO_ORO = "GLD"

# Cada cuantos minutos mandamos el reporte de estado del oro en USD y EUR
INTERVALO_ORO_MINUTOS = 10

# Cada cuantas horas mandamos el aviso de tendencia tecnica del oro
INTERVALO_TENDENCIA_ORO_HORAS = 1

# Ventanas para la tendencia intradia (xaus.com muestrea cada 2 minutos)
VENTANA_CORTA_MINUTOS = 30   # promedio "reciente"
VENTANA_LARGA_HORAS = 4      # promedio "de fondo" para comparar

# --- Sistema primario / backup ---
ROL_BOT = os.environ.get("ROL_BOT", "primario").lower()  # "primario" o "backup"
URL_BOT_PRIMARIO = os.environ.get("URL_BOT_PRIMARIO", "")
MINUTOS_SIN_RESPUESTA_PARA_RELEVO = 10  # cuanto esperar antes de que el backup tome el relevo

# Si el primario esta en un hosting gratis (Render, etc.) que lo duerme por
# inactividad, la primera peticion para "despertarlo" puede tardar bastante
# en responder mientras el contenedor arranca. Con un timeout corto, el
# backup lo daria por caido en ese primer intento aunque solo estuviera
# despertando. Por eso usamos un timeout amplio y un par de reintentos.
TIMEOUT_PING_PRIMARIO_SEGUNDOS = 30
REINTENTOS_PING_PRIMARIO = 2
ESPERA_ENTRE_REINTENTOS_SEGUNDOS = 8

MADRID_TZ = ZoneInfo("Europe/Madrid")

# El backup arranca en silencio; solo se activa si el primario deja de responder
_modo_activo = (ROL_BOT != "backup")
_primario_caido_desde = None

# Palabras clave para el filtro de noticias generales (geopolitica, Fed, dolar...)
KEYWORDS_GEOPOLITICA = [
    "war", "guerra", "conflict", "sanctions", "sanciones",
    "military strike", "ataque militar", "tension", "invasion"
]
KEYWORDS_FED = [
    "federal reserve", "fomc", "interest rate", "powell", "rate hike", "rate cut"
]
KEYWORDS_DOLAR = ["dollar index", "dxy", "dollar weakens", "dollar strengthens"]

# Guarda IDs/timestamps ya notificados para no duplicar alertas
_ya_notificado = set()

# Guarda el momento (datetime) de las ultimas noticias enviadas, para poder
# cruzarlas despues con los picos de liquidez que detectemos
_ultimas_noticias = deque(maxlen=30)

# Historial reciente de spread y volumen del SPY, para calcular una media movil
_historial_spread = deque(maxlen=20)
_historial_volumen = deque(maxlen=20)


# ---------------------------------------------------------------------------
# UTILIDADES
# ---------------------------------------------------------------------------

def hora_madrid():
    return datetime.now(MADRID_TZ).strftime("%d/%m/%Y %H:%M:%S")


def en_pausa_fin_de_semana():
    """Los mercados (bolsa, oro, dolar) cierran el viernes en la noche y no
    vuelven a abrir hasta el domingo en la noche. En ese lapso no tiene
    sentido gastar consultas de las APIs -- no habria ningun dato nuevo
    que reportar de todas formas."""
    ahora = datetime.now(MADRID_TZ)
    dia = ahora.weekday()  # lunes=0 ... domingo=6
    hora = ahora.hour
    if dia == 4 and hora >= 22:   # viernes desde las 22:00
        return True
    if dia == 5:                  # sabado, todo el dia
        return True
    if dia == 6 and hora < 22:    # domingo, hasta las 22:00
        return True
    return False


def saltar_en_fin_de_semana(func):
    """Decorador: si estamos en la pausa de fin de semana, no ejecuta la
    funcion (solo lo avisa por consola, no manda nada a Discord)."""
    def envoltura(*args, **kwargs):
        if en_pausa_fin_de_semana():
            print(f"En pausa de fin de semana -- se salta {func.__name__}()")
            return
        return func(*args, **kwargs)
    envoltura.__name__ = func.__name__
    return envoltura


def saltar_si_backup_en_silencio(func):
    """Decorador: si este bot es el 'backup' y el primario sigue respondiendo
    (modo silencio), NO ejecuta la funcion. Antes, el backup igual consultaba
    todas las APIs externas (FRED, EIA, GDELT, NewsAPI, Alpaca, xaus.com) y
    solo se descartaba el mensaje final -- eso gasta cuota diaria de las APIs
    gratis sin necesidad, mientras el primario ya esta cubriendo lo mismo."""
    def envoltura(*args, **kwargs):
        if ROL_BOT == "backup" and not _modo_activo:
            print(f"Backup en silencio -- se salta {func.__name__}() (se ahorra cuota de API)")
            return
        return func(*args, **kwargs)
    envoltura.__name__ = func.__name__
    return envoltura


def traducir(texto):
    """Traduce un texto (normalmente en ingles) al espanol. Si falla por
    cualquier motivo (sin internet, servicio caido, texto vacio), devuelve
    el texto original en vez de romper el bot."""
    if not texto:
        return texto
    try:
        return GoogleTranslator(source="auto", target="es").translate(texto)
    except Exception as e:
        print(f"Error al traducir: {e}")
        return texto


def enviar_discord(titulo, descripcion, color=0xF5A623, es_noticia=False):
    """Publica un embed en el canal de Discord configurado.

    es_noticia=True marca el envio como una noticia real (no una alerta de
    liquidez ni el mensaje de arranque), para poder cruzarla despues con
    picos de volumen o spread.

    Si este bot es el "backup" y el primario sigue respondiendo bien, se
    queda en silencio (no manda nada) para no duplicar mensajes.
    """
    if ROL_BOT == "backup" and not _modo_activo:
        print(f"[BACKUP EN SILENCIO -- primario activo] {titulo}")
        return

    if es_noticia:
        _ultimas_noticias.append(datetime.now(MADRID_TZ))

    if "PON_AQUI" in DISCORD_WEBHOOK_URL:
        print(f"[SIN CONFIGURAR] {titulo}: {descripcion}")
        return
    payload = {
        "embeds": [{
            "title": titulo,
            "description": descripcion,
            "color": color,
            "footer": {"text": f"Hora Madrid: {hora_madrid()} | Bot: {ROL_BOT}"}
        }]
    }
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Error enviando a Discord: {e}")


# ---------------------------------------------------------------------------
# FUENTES DE DATOS
# ---------------------------------------------------------------------------

@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_fed_tipos():
    """FRED: tipo de interes efectivo de la Fed (Federal Funds Rate)."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "FEDFUNDS",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1
    }
    try:
        data = requests.get(url, params=params, timeout=10).json()
        obs = data["observations"][0]
        clave = f"fed_{obs['date']}"
        if clave not in _ya_notificado:
            _ya_notificado.add(clave)
            enviar_discord(
                "Tipo de interes de la Fed (dato FRED)",
                f"Federal Funds Rate: {obs['value']}% (dato de {obs['date']})",
                color=0x378ADD,
                es_noticia=True
            )
    except Exception as e:
        print(f"Error FRED tipos: {e}")


@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_desempleo():
    """FRED: tasa de desempleo (UNRATE)."""
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "UNRATE",
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 1
    }
    try:
        data = requests.get(url, params=params, timeout=10).json()
        obs = data["observations"][0]
        clave = f"unrate_{obs['date']}"
        if clave not in _ya_notificado:
            _ya_notificado.add(clave)
            enviar_discord(
                "Tasa de desempleo EEUU (dato FRED)",
                f"Desempleo: {obs['value']}% (dato de {obs['date']})",
                color=0x7F77DD,
                es_noticia=True
            )
    except Exception as e:
        print(f"Error FRED desempleo: {e}")


@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_petroleo():
    """EIA: precio spot del WTI."""
    url = "https://api.eia.gov/v2/petroleum/pri/spt/data/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "daily",
        "data[0]": "value",
        "facets[series][]": "RWTC",
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "length": 1
    }
    try:
        data = requests.get(url, params=params, timeout=10).json()
        row = data["response"]["data"][0]
        clave = f"wti_{row['period']}"
        if clave not in _ya_notificado:
            _ya_notificado.add(clave)
            enviar_discord(
                "Precio del petroleo WTI (EIA)",
                f"WTI: ${row['value']} (dato de {row['period']})",
                color=0xF5A623,
                es_noticia=True
            )
    except Exception as e:
        print(f"Error EIA petroleo: {e}")


@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_geopolitica():
    """GDELT: eventos globales recientes filtrados por palabras clave de conflicto."""
    url = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": "(war OR sanctions OR military conflict) sourcelang:english",
        "mode": "artlist",
        "maxrecords": 5,
        "format": "json",
        "sort": "datedesc"
    }
    try:
        data = requests.get(url, params=params, timeout=10).json()
        for art in data.get("articles", []):
            clave = art.get("url")
            if clave and clave not in _ya_notificado:
                _ya_notificado.add(clave)
                titulo_es = traducir(art.get('title'))
                enviar_discord(
                    "Alerta geopolitica",
                    f"{titulo_es}\n{art.get('url')}",
                    color=0xE24B4A,
                    es_noticia=True
                )
    except Exception as e:
        print(f"Error GDELT: {e}")


@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_noticias_generales():
    """NewsAPI (opcional): titulares sobre Fed / dolar, si configuraste tu API key."""
    if not NEWSAPI_KEY:
        return
    url = "https://newsapi.org/v2/everything"
    todas = KEYWORDS_FED + KEYWORDS_DOLAR
    params = {
        "q": " OR ".join(todas),
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 5,
        "apiKey": NEWSAPI_KEY
    }
    try:
        data = requests.get(url, params=params, timeout=10).json()
        for art in data.get("articles", []):
            clave = art.get("url")
            if clave and clave not in _ya_notificado:
                _ya_notificado.add(clave)
                titulo_es = traducir(art.get('title'))
                enviar_discord(
                    "Noticia Fed / dolar",
                    f"{titulo_es}\n{art.get('url')}",
                    color=0x1D9E75,
                    es_noticia=True
                )
    except Exception as e:
        print(f"Error NewsAPI: {e}")


# ---------------------------------------------------------------------------
# LIQUIDEZ (volumen y spread bid-ask, cruzado con noticias recientes)
# ---------------------------------------------------------------------------

# Umbrales: cuanto tiene que dispararse el spread/volumen sobre su media
# reciente para considerarlo un movimiento real. Ajusta segun tu tolerancia.
UMBRAL_SPREAD = 1.8   # spread actual > 1.8x la media reciente
UMBRAL_VOLUMEN = 2.0  # volumen actual > 2x la media reciente
MINUTOS_CORRELACION_NOTICIA = 10  # ventana para considerar "coincide con noticia"


def _noticia_reciente():
    """Devuelve la noticia mas reciente si cayo dentro de la ventana de correlacion."""
    ahora = datetime.now(MADRID_TZ)
    for momento in reversed(_ultimas_noticias):
        minutos = (ahora - momento).total_seconds() / 60
        if minutos <= MINUTOS_CORRELACION_NOTICIA:
            return momento
    return None


# ---------------------------------------------------------------------------
# LIQUIDEZ: fase 2 (revision del snapshot)
# ---------------------------------------------------------------------------
@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_liquidez():
    """Alpaca: consulta el snapshot del SPY (spread bid-ask + volumen del minuto)
    y lo compara contra su propia media reciente para detectar movimientos de
    liquidez fuera de lo normal, cruzandolos con las ultimas noticias enviadas."""
    if "PON_AQUI" in ALPACA_API_KEY:
        return

    url = f"https://data.alpaca.markets/v2/stocks/{SIMBOLO_LIQUIDEZ}/snapshot"
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
    }
    try:
        data = requests.get(url, headers=headers, timeout=10).json()
        quote = data["latestQuote"]
        bar = data["minuteBar"]

        spread_actual = round(quote["ap"] - quote["bp"], 4)
        volumen_actual = bar["v"]

        media_spread = statistics.mean(_historial_spread) if _historial_spread else None
        media_volumen = statistics.mean(_historial_volumen) if _historial_volumen else None

        spread_disparado = media_spread and spread_actual > media_spread * UMBRAL_SPREAD
        volumen_disparado = media_volumen and volumen_actual > media_volumen * UMBRAL_VOLUMEN

        if spread_disparado or volumen_disparado:
            noticia = _noticia_reciente()
            if noticia:
                titulo = "Movimiento de liquidez -- coincide con una noticia reciente"
                color = 0xE24B4A
                contexto = f"Hay una noticia enviada hace {round((datetime.now(MADRID_TZ) - noticia).total_seconds() / 60, 1)} min. Probablemente relacionado."
            else:
                titulo = "Movimiento de liquidez -- sin noticia aparente"
                color = 0xEF9F27
                contexto = "No hay ninguna noticia reciente registrada por el bot. Puede ser ruido, un dato no capturado, o un movimiento tecnico."

            detalle = []
            if spread_disparado:
                detalle.append(f"Spread bid-ask: {spread_actual} (media reciente: {round(media_spread, 4)})")
            if volumen_disparado:
                detalle.append(f"Volumen del minuto: {volumen_actual} (media reciente: {round(media_volumen)})")

            enviar_discord(
                titulo,
                f"{SIMBOLO_LIQUIDEZ}\n" + "\n".join(detalle) + f"\n\n{contexto}",
                color=color
            )

        _historial_spread.append(spread_actual)
        _historial_volumen.append(volumen_actual)

    except Exception as e:
        print(f"Error Alpaca liquidez: {e}")


# ---------------------------------------------------------------------------
# DOLAR: caida o ruptura del maximo de la semana
# ---------------------------------------------------------------------------

def _obtener_dolar_datos():
    """Devuelve (precio_actual, maximo_semanal_incluido_hoy, maximo_semanal_previo)
    del proxy del dolar (UUP), o None si algo falla."""
    headers = {
        "APCA-API-KEY-ID": ALPACA_API_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
    }
    desde = (datetime.now(MADRID_TZ) - timedelta(days=7)).strftime("%Y-%m-%d")
    url_bars = f"https://data.alpaca.markets/v2/stocks/{SIMBOLO_DOLAR}/bars"
    params = {"timeframe": "1Day", "start": desde, "limit": 10}
    data_bars = requests.get(url_bars, headers=headers, params=params, timeout=10).json()
    barras = data_bars.get("bars", [])
    if not barras:
        return None

    # La ultima barra es la de hoy (aun formandose); la usamos para el precio
    # actual y la excluimos al calcular el maximo "previo" (para poder
    # distinguir una ruptura de hoy del maximo que ya traia la semana)
    maximo_semanal = max(b["h"] for b in barras)
    maximo_previo = max((b["h"] for b in barras[:-1]), default=barras[0]["h"])

    url_snap = f"https://data.alpaca.markets/v2/stocks/{SIMBOLO_DOLAR}/snapshot"
    snap = requests.get(url_snap, headers=headers, timeout=10).json()
    precio_actual = snap["latestTrade"]["p"]

    return precio_actual, maximo_semanal, maximo_previo


def _obtener_cambio_oro():
    """Devuelve el % de cambio del oro (precio real XAU/USD via xaus.com) en
    el ultimo dia, o None si algo falla. Se usa para la correlacion inversa con el dolar."""
    try:
        data = requests.get("https://xaus.com/api/v1/history", params={"symbol": "xau"}, timeout=10).json()
        puntos = data.get("points", [])
        if len(puntos) < 2:
            return None
        precio_actual = puntos[-1]["c"]
        cierre_anterior = puntos[-2]["c"]
        return round((precio_actual - cierre_anterior) / cierre_anterior * 100, 2)
    except Exception as e:
        print(f"Error XAUS oro: {e}")
        return None


@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_dolar_maximo_semanal():
    """Avisa si el dolar (proxy UUP) cae mas del umbral desde el maximo de la
    semana, o si rompe al alza el maximo que traia la semana hasta hoy."""
    if "PON_AQUI" in ALPACA_API_KEY:
        return

    try:
        datos = _obtener_dolar_datos()
        if datos is None:
            return
        precio_actual, maximo_semanal, maximo_previo = datos
        hoy = datetime.now(MADRID_TZ).strftime("%Y-%m-%d")

        # --- Caida desde el maximo de la semana ---
        caida_pct = round((maximo_semanal - precio_actual) / maximo_semanal * 100, 2)
        clave_caida = f"usd_caida_{hoy}_{int(caida_pct)}"
        if caida_pct >= UMBRAL_CAIDA_DOLAR and clave_caida not in _ya_notificado:
            _ya_notificado.add(clave_caida)

            cambio_oro = _obtener_cambio_oro()
            if cambio_oro is None:
                linea_oro = "No se pudo consultar el oro en este momento."
            elif cambio_oro > 0:
                linea_oro = f"El oro (XAU/USD) responde: subio {cambio_oro}% -- coherente con la relacion inversa dolar-oro."
            elif cambio_oro < 0:
                linea_oro = f"El oro (XAU/USD) responde: bajo {abs(cambio_oro)}% -- no sigue la relacion inversa habitual, revisalo."
            else:
                linea_oro = "El oro se mantiene practicamente sin cambios."

            enviar_discord(
                "El dolar acaba de tener un pico a la baja",
                f"{SIMBOLO_DOLAR} (proxy del indice dolar)\n"
                f"Maximo de la semana: {maximo_semanal}\n"
                f"Precio actual: {precio_actual}\n"
                f"Caida: {caida_pct}%\n\n"
                f"{linea_oro}",
                color=0x5DCAA5,
                es_noticia=True
            )

        # --- Ruptura al alza del maximo que traia la semana ---
        subida_pct = round((precio_actual - maximo_previo) / maximo_previo * 100, 2)
        clave_subida = f"usd_subida_{hoy}_{int(subida_pct)}"
        if precio_actual > maximo_previo and subida_pct >= UMBRAL_SUBIDA_DOLAR and clave_subida not in _ya_notificado:
            _ya_notificado.add(clave_subida)
            enviar_discord(
                "El dolar supera su maximo semanal",
                f"{SIMBOLO_DOLAR} (proxy del indice dolar)\n"
                f"Maximo previo de la semana: {maximo_previo}\n"
                f"Precio actual: {precio_actual}\n"
                f"Nueva subida: {subida_pct}%",
                color=0xE24B4A,
                es_noticia=True
            )
    except Exception as e:
        print(f"Error Alpaca dolar: {e}")


# ---------------------------------------------------------------------------
# ORO: reporte periodico en USD y EUR (XAU/USD y XAU/EUR, precio real -- xaus.com)
# ---------------------------------------------------------------------------

def _obtener_gld_alpaca():
    """Alpaca: precio y cambio del dia del ETF GLD (segunda fuente/referencia
    del oro). Devuelve (precio, cambio_pct) o (None, None) si falla."""
    if "PON_AQUI" in ALPACA_API_KEY:
        return None, None
    try:
        headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
        }
        url = f"https://data.alpaca.markets/v2/stocks/{SIMBOLO_ORO}/snapshot"
        snap = requests.get(url, headers=headers, timeout=10).json()
        precio = snap["latestTrade"]["p"]
        cierre_anterior = snap["prevDailyBar"]["c"]
        cambio_pct = round((precio - cierre_anterior) / cierre_anterior * 100, 2)
        return precio, cambio_pct
    except Exception as e:
        print(f"Error Alpaca GLD: {e}")
        return None, None


@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_estado_oro():
    """Manda a Discord el estado del oro combinando DOS fuentes en el mismo
    mensaje: el precio real XAU/USD y XAU/EUR (xaus.com) y el ETF GLD como
    referencia adicional (Alpaca) -- asi si una fuente falla, la otra sigue
    dando informacion, y de paso se pueden comparar entre si."""
    lineas = []

    # --- Fuente 1: xaus.com (precio real de la onza) ---
    try:
        resp_usd = requests.get("https://xaus.com/api/v1/spot", timeout=10).json()
        precio_usd = resp_usd["xau"]["price"]

        resp_eur = requests.get("https://xaus.com/api/v1/spot", params={"currency": "EUR"}, timeout=10).json()
        precio_eur = resp_eur["xau"]["price"]

        cambio_pct = _obtener_cambio_oro()
        cambio_txt = f" ({'+' if cambio_pct and cambio_pct > 0 else ''}{cambio_pct}% hoy)" if cambio_pct is not None else ""

        lineas.append(
            f"Fuente 1 -- xaus.com (precio real):\n"
            f"XAU/USD: {precio_usd} USD por onza troy\n"
            f"XAU/EUR: {precio_eur} EUR por onza troy{cambio_txt}"
        )
    except Exception as e:
        print(f"Error XAUS estado oro: {e}")
        lineas.append("Fuente 1 -- xaus.com: no se pudo consultar en este momento.")

    # --- Fuente 2: Alpaca (ETF GLD, como referencia adicional) ---
    precio_gld, cambio_gld = _obtener_gld_alpaca()
    if precio_gld is not None:
        cambio_txt = f" ({'+' if cambio_gld > 0 else ''}{cambio_gld}% hoy)" if cambio_gld is not None else ""
        lineas.append(f"\nFuente 2 -- Alpaca (ETF {SIMBOLO_ORO}):\n{precio_gld} USD{cambio_txt}")
    else:
        lineas.append(f"\nFuente 2 -- Alpaca (ETF {SIMBOLO_ORO}): no se pudo consultar en este momento.")

    enviar_discord(
        "Estado del oro -- dos fuentes (xaus.com + Alpaca)",
        "\n".join(lineas),
        color=0xE8B84B
    )


# ---------------------------------------------------------------------------
# ORO: tendencia tecnica (medias moviles) -- NO es prediccion ni consejo financiero
# ---------------------------------------------------------------------------

@saltar_en_fin_de_semana
@saltar_si_backup_en_silencio
def revisar_tendencia_oro():
    """Calcula una tendencia tecnica del oro usando datos INTRADIA de xaus.com
    (muestreados cada 2 minutos), comparando un promedio reciente (ultimos
    VENTANA_CORTA_MINUTOS) contra un promedio de fondo (ultimas VENTANA_LARGA_HORAS).
    Es el mismo tipo de patron tecnico (cruce de medias moviles) pero mas
    sensible al corto plazo -- NO es una prediccion garantizada ni consejo
    financiero, el precio puede romper el patron en cualquier momento."""
    try:
        data = requests.get(
            "https://xaus.com/api/v1/intraday",
            params={"symbol": "xau", "hours": VENTANA_LARGA_HORAS},
            timeout=10
        ).json()
        puntos = data.get("points", [])

        precios = [p["p"] for p in puntos]
        minimo_puntos_corta = max(1, VENTANA_CORTA_MINUTOS // 2)  # ~1 punto cada 2 min

        if len(precios) < minimo_puntos_corta + 1:
            print("Tendencia oro: no hay suficientes datos intradia todavia.")
            return

        media_corta = round(statistics.mean(precios[-minimo_puntos_corta:]), 2)
        media_larga = round(statistics.mean(precios), 2)
        precio_actual = precios[-1]

        diferencia_pct = round((media_corta - media_larga) / media_larga * 100, 2)

        if media_corta > media_larga:
            tendencia = "AL ALZA"
            explicacion = (
                f"El promedio de los ultimos {VENTANA_CORTA_MINUTOS} minutos ({media_corta}) "
                f"esta por encima del promedio de las ultimas {VENTANA_LARGA_HORAS} horas ({media_larga}), "
                f"un patron tecnico de tendencia alcista a corto plazo."
            )
            color = 0x5DCAA5
        elif media_corta < media_larga:
            tendencia = "A LA BAJA"
            explicacion = (
                f"El promedio de los ultimos {VENTANA_CORTA_MINUTOS} minutos ({media_corta}) "
                f"esta por debajo del promedio de las ultimas {VENTANA_LARGA_HORAS} horas ({media_larga}), "
                f"un patron tecnico de tendencia bajista a corto plazo."
            )
            color = 0xE24B4A
        else:
            tendencia = "LATERAL / SIN TENDENCIA CLARA"
            explicacion = "Los dos promedios estan practicamente iguales, no hay una direccion clara."
            color = 0xEF9F27

        enviar_discord(
            f"Tendencia tecnica del oro (intradia): {tendencia}",
            f"Precio actual: {precio_actual} USD/oz\n"
            f"Promedio {VENTANA_CORTA_MINUTOS} min: {media_corta} | "
            f"Promedio {VENTANA_LARGA_HORAS}h: {media_larga} (diferencia: {diferencia_pct}%)\n\n"
            f"{explicacion}\n\n"
            f"IMPORTANTE: esto es un patron tecnico de corto plazo (cruce de "
            f"promedios intradia), no es una prediccion garantizada ni consejo "
            f"financiero. El precio puede cambiar de direccion en cualquier "
            f"momento por noticias, datos economicos o eventos geopoliticos.",
            color=color
        )
    except Exception as e:
        print(f"Error tendencia oro intradia: {e}")


# ---------------------------------------------------------------------------
# PROGRAMACION (todo en hora de Madrid)
# ---------------------------------------------------------------------------

def ejecutar_todas():
    revisar_fed_tipos()
    revisar_desempleo()
    revisar_petroleo()
    revisar_geopolitica()
    revisar_noticias_generales()
    revisar_liquidez()
    revisar_dolar_maximo_semanal()
    revisar_estado_oro()
    revisar_tendencia_oro()


# ---------------------------------------------------------------------------
# PRUEBA MANUAL: manda a Discord los datos reales de ahora mismo (sin
# esperar a que se cumpla ningun umbral), para confirmar que Alpaca funciona
# ---------------------------------------------------------------------------

def enviar_prueba_completa():
    lineas = []

    if "PON_AQUI" not in ALPACA_API_KEY:
        headers = {
            "APCA-API-KEY-ID": ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY
        }
        for simbolo in [SIMBOLO_LIQUIDEZ, SIMBOLO_DOLAR, SIMBOLO_ORO]:
            try:
                url = f"https://data.alpaca.markets/v2/stocks/{simbolo}/snapshot"
                snap = requests.get(url, headers=headers, timeout=10).json()
                precio = snap.get("latestTrade", {}).get("p", "sin dato")
                lineas.append(f"{simbolo} (Alpaca): ultimo precio {precio}")
            except Exception as e:
                lineas.append(f"{simbolo} (Alpaca): error -- {e}")
    else:
        lineas.append("Alpaca: faltan ALPACA_API_KEY / ALPACA_SECRET_KEY.")

    try:
        resp = requests.get("https://xaus.com/api/v1/spot", timeout=10).json()
        lineas.append(f"XAU/USD (xaus.com): {resp['xau']['price']} USD por onza")
    except Exception as e:
        lineas.append(f"XAU/USD (xaus.com): error -- {e}")

    enviar_discord(
        "Prueba de conexion -- datos en vivo",
        "Estos son los datos reales que el bot esta leyendo ahora mismo:\n" + "\n".join(lineas),
        color=0x639922
    )


# ---------------------------------------------------------------------------
# SISTEMA PRIMARIO / BACKUP: el backup vigila al primario y toma el relevo
# si deja de responder (solo corre esta logica si ROL_BOT == "backup")
# ---------------------------------------------------------------------------

def _despertar_y_verificar_primario():
    """Intenta contactar (y de paso 'despertar') al bot primario. Si el
    hosting gratis lo tenia dormido, la primera peticion sirve para
    reactivarlo pero puede no llegar a tiempo -- por eso reintentamos un
    par de veces con una pausa corta, antes de darlo por caido de verdad."""
    for intento in range(1, REINTENTOS_PING_PRIMARIO + 1):
        try:
            r = requests.get(URL_BOT_PRIMARIO, timeout=TIMEOUT_PING_PRIMARIO_SEGUNDOS)
            if r.status_code == 200:
                return True
            print(f"Ping primario intento {intento}: respondio con status {r.status_code}")
        except requests.RequestException as e:
            print(f"Ping primario intento {intento}: sin respuesta ({e})")

        if intento < REINTENTOS_PING_PRIMARIO:
            time.sleep(ESPERA_ENTRE_REINTENTOS_SEGUNDOS)

    return False


def revisar_bot_primario():
    global _modo_activo, _primario_caido_desde

    if ROL_BOT != "backup" or not URL_BOT_PRIMARIO:
        return

    primario_responde = _despertar_y_verificar_primario()

    ahora = datetime.now(MADRID_TZ)

    if primario_responde:
        if _modo_activo:
            # El primario volvio -- el backup se calla otra vez
            enviar_discord_forzado(
                "Backup: el bot primario volvio",
                "El bot primario ya responde de nuevo. Este backup vuelve a quedarse en silencio.",
                color=0x639922
            )
        _modo_activo = False
        _primario_caido_desde = None
    else:
        if _primario_caido_desde is None:
            _primario_caido_desde = ahora
        minutos_caido = (ahora - _primario_caido_desde).total_seconds() / 60

        if minutos_caido >= MINUTOS_SIN_RESPUESTA_PARA_RELEVO and not _modo_activo:
            _modo_activo = True
            enviar_discord_forzado(
                "Backup: tomando el relevo",
                f"El bot primario lleva sin responder mas de {MINUTOS_SIN_RESPUESTA_PARA_RELEVO} "
                f"minutos. Este bot de backup toma el relevo y empieza a mandar las alertas.",
                color=0xE24B4A
            )


def enviar_discord_forzado(titulo, descripcion, color):
    """Igual que enviar_discord, pero ignora el modo silencio -- se usa solo
    para los avisos de 'tomo el relevo' / 'el primario volvio'."""
    if "PON_AQUI" in DISCORD_WEBHOOK_URL:
        print(f"[SIN CONFIGURAR] {titulo}: {descripcion}")
        return
    payload = {
        "embeds": [{
            "title": titulo,
            "description": descripcion,
            "color": color,
            "footer": {"text": f"Hora Madrid: {hora_madrid()} | Bot: {ROL_BOT}"}
        }]
    }
    try:
        r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"Error enviando a Discord: {e}")


# ---------------------------------------------------------------------------
# MINI SERVIDOR WEB (solo para que hosts gratuitos como Render sepan que el
# bot sigue vivo -- no hace falta usarlo si corres el script en tu ordenador)
# ---------------------------------------------------------------------------

def iniciar_servidor_web():
    puerto = int(os.environ.get("PORT", 8080))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Bot de mercado activo")

        def do_HEAD(self):
            # UptimeRobot (y otros monitores) mandan peticiones HEAD por
            # defecto -- sin este metodo, Python responde 501 No Implementado
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()

        def log_message(self, format, *args):
            pass  # para no llenar los logs con cada ping

    servidor = HTTPServer(("0.0.0.0", puerto), Handler)
    servidor.serve_forever()


if __name__ == "__main__":
    # Mini servidor web PRIMERO QUE NADA -- asi Render/UptimeRobot detectan
    # el bot vivo de inmediato, sin esperar a que termine de mandar mensajes
    # o consultar APIs (eso evita que se quede "dormido" por no responder a tiempo)
    threading.Thread(target=iniciar_servidor_web, daemon=True).start()

    print("Bot de monitor de mercado iniciado. Hora Madrid:", hora_madrid())

    # Aviso de arranque: se manda SIEMPRE (incluso si es backup en silencio),
    # usando enviar_discord_forzado -- si no, nunca sabrias que el backup se
    # desplego correctamente hasta que tuviera que tomar el relevo.
    if ROL_BOT == "backup":
        enviar_discord_forzado(
            "Backup iniciado (en silencio)",
            f"Este bot arranco en modo BACKUP, vigilando a: {URL_BOT_PRIMARIO or '(URL_BOT_PRIMARIO no configurada)'}\n"
            f"Se mantendra en silencio y sin consultar las APIs mientras el primario responda. "
            f"Si el primario deja de responder mas de {MINUTOS_SIN_RESPUESTA_PARA_RELEVO} minutos, "
            f"este bot tomara el relevo automaticamente.",
            color=0x639922
        )
    else:
        enviar_discord("Bot iniciado", "El monitor de mercado de EEUU esta activo.", color=0x639922)
        enviar_prueba_completa()

    scheduler = BlockingScheduler(timezone=MADRID_TZ)

    # Datos economicos (Fed, desempleo, petroleo): cambian poco, revision cada hora
    scheduler.add_job(revisar_fed_tipos, "interval", hours=1)
    scheduler.add_job(revisar_desempleo, "interval", hours=1)
    scheduler.add_job(revisar_petroleo, "interval", hours=1)

    # Geopolitica y noticias generales: mas frecuente, cada 10 minutos
    scheduler.add_job(revisar_geopolitica, "interval", minutes=10)
    scheduler.add_job(revisar_noticias_generales, "interval", minutes=10)

    # Liquidez (volumen/spread): el mercado se mueve rapido, revision cada 2 minutos
    scheduler.add_job(revisar_liquidez, "interval", minutes=2)

    # Dolar vs maximo semanal: no hace falta revisarlo tan a menudo, cada 30 minutos
    scheduler.add_job(revisar_dolar_maximo_semanal, "interval", minutes=30)

    # Estado del oro en USD/EUR: reporte periodico, mas frecuente si quieres
    scheduler.add_job(revisar_estado_oro, "interval", minutes=INTERVALO_ORO_MINUTOS)

    # Tendencia tecnica del oro (medias moviles): el historial es diario, no
    # hace falta revisarlo tan seguido
    scheduler.add_job(revisar_tendencia_oro, "interval", hours=INTERVALO_TENDENCIA_ORO_HORAS)

    # Vigilancia del bot primario (solo hace algo si ROL_BOT="backup")
    scheduler.add_job(revisar_bot_primario, "interval", minutes=5)

    # Revision inmediata al arrancar
    ejecutar_todas()

    scheduler.start()
