# ══════════════════════════════════════════════════════
# ETL: API Clima Lima → Supabase (nube gratis)
# ══════════════════════════════════════════════════════

import requests                        # llama a la API
import pandas as pd                    # transforma datos
from sqlalchemy import create_engine   # conecta a Supabase
from dotenv     import load_dotenv     # lee el archivo .env
from datetime   import datetime        # registra la hora
import os                              # lee variables de entorno

# ══════════════════════════════════════════════════════
# CONFIGURACIÓN
# ══════════════════════════════════════════════════════
load_dotenv()  # carga el archivo .env

SUPABASE_URL = os.getenv("SUPABASE_URL")  # tu URL de Supabase
TABLA        = "clima_lima"               # nombre de la tabla
ARCHIVO_CSV  = "clima_lima.csv"           # archivo local temporal

# ══════════════════════════════════════════════════════
# EXTRACCIÓN → API Open Meteo (gratis, sin registro)
# ══════════════════════════════════════════════════════
print(f"\n{'='*50}")
print(f"ETL iniciado: {datetime.now()}")
print(f"{'='*50}")

url = "https://api.open-meteo.com/v1/forecast"

# Le decimos a la API qué datos queremos de Lima
parametros = {
    "latitude":  -12.0464,     # latitud Lima
    "longitude": -77.0428,     # longitud Lima
    "daily": [
        "temperature_2m_max",  # temperatura máxima
        "temperature_2m_min",  # temperatura mínima
        "precipitation_sum",   # lluvia en mm
        "windspeed_10m_max"    # velocidad del viento
    ],
    "timezone":  "America/Lima",
    "past_days": 30            # últimos 30 días
}

# Llamamos a la API
respuesta = requests.get(url, params=parametros)

# Verificamos que respondió bien
if respuesta.status_code == 200:
    data = respuesta.json()
    print("1️⃣  Extracción completada ✅")
    print(f"    → Días extraídos : {len(data['daily']['time'])}")
else:
    print(f"❌ Error en API: {respuesta.status_code}")
    exit()

# ══════════════════════════════════════════════════════
# TRANSFORMACIÓN → limpieza y enriquecimiento
# ══════════════════════════════════════════════════════

# Convertimos la respuesta de la API a un DataFrame
df = pd.DataFrame({
    "fecha":      data['daily']['time'],
    "temp_max":   data['daily']['temperature_2m_max'],
    "temp_min":   data['daily']['temperature_2m_min'],
    "lluvia_mm":  data['daily']['precipitation_sum'],
    "viento_max": data['daily']['windspeed_10m_max']
})

# ── Limpieza ──────────────────────────────────────────

# Fecha de texto a formato fecha real
df['fecha'] = pd.to_datetime(df['fecha'])

# Rellenamos nulos
df['lluvia_mm']  = df['lluvia_mm'].fillna(0)
df['viento_max'] = df['viento_max'].fillna(0)
df['temp_max']   = df['temp_max'].fillna(df['temp_max'].mean())
df['temp_min']   = df['temp_min'].fillna(df['temp_min'].mean())

# Redondeamos decimales
df['temp_max']   = df['temp_max'].round(1)
df['temp_min']   = df['temp_min'].round(1)
df['lluvia_mm']  = df['lluvia_mm'].round(2)
df['viento_max'] = df['viento_max'].round(1)

# ── Columnas nuevas ───────────────────────────────────

# Extraemos partes de la fecha
df['año']        = df['fecha'].dt.year
df['mes']        = df['fecha'].dt.month
df['dia']        = df['fecha'].dt.day
df['nombre_mes'] = df['fecha'].dt.strftime('%B')
df['dia_semana'] = df['fecha'].dt.strftime('%A')

# Temperatura promedio del día
df['temp_promedio'] = ((df['temp_max'] + df['temp_min']) / 2).round(1)

# Clasificamos el día según lluvia
df['tipo_dia'] = df['lluvia_mm'].apply(
    lambda x: 'Lluvioso' if x > 5 else 'Nublado' if x > 0 else 'Soleado'
)

# Clasificamos el viento
df['tipo_viento'] = df['viento_max'].apply(
    lambda x: 'Fuerte' if x > 40 else 'Moderado' if x > 20 else 'Suave'
)

# Datos fijos
df['ciudad'] = 'Lima'
df['pais']   = 'Peru'

print("2️⃣  Transformación completada ✅")
print(f"    → Filas     : {len(df)}")
print(f"    → Columnas  : {len(df.columns)}")
print(f"    → Nulos     : {df.isnull().sum().sum()}")
print(df.head(3))

# ══════════════════════════════════════════════════════
# CARGA → Supabase (nube gratis)
# ══════════════════════════════════════════════════════
try:
    # Nos conectamos a Supabase con la URL del .env
    engine = create_engine(SUPABASE_URL)

    # Cargamos el DataFrame a Supabase
    df.to_sql(
        name      = TABLA,      # nombre de la tabla
        con       = engine,     # conexión a Supabase
        if_exists = "replace",  # reemplaza si ya existe
        index     = False       # no guarda índice de pandas
    )

    # Verificamos que llegaron los datos
    total = pd.read_sql(f"SELECT COUNT(*) as total FROM {TABLA}", engine)
    print("3️⃣  Cargado en Supabase ✅")
    print(f"    → Registros en nube: {total['total'][0]}")

except Exception as e:
    print(f"❌ Error Supabase: {e}")
    exit()

# ══════════════════════════════════════════════════════
# RESUMEN FINAL
# ══════════════════════════════════════════════════════
print(f"\n{'='*50}")
print(f"ETL finalizado: {datetime.now()} ✅")
print(f"{'='*50}")
print(f"✅ Registros procesados : {len(df)}")
print(f"✅ Columnas generadas   : {len(df.columns)}")
print(f"✅ Tabla en Supabase    : {TABLA}")
print(f"{'='*50}\n")