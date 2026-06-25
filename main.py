import shutil
import os
import logging
import datetime
import zipfile
import hashlib
import boto3
import cv2 
import numpy as np
import tempfile 
import smtplib
import pytz
import json
import io
import difflib
import psycopg2
from psycopg2.extras import RealDictCursor
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional, List
from fastapi import FastAPI, UploadFile, Form, HTTPException, BackgroundTasks, Request, File
from fastapi.middleware.cors import CORSMiddleware
from passlib.context import CryptContext
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from botocore.config import Config
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
CLAVE_SUPREMA = "Despertar2026"

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_password_hash(password):
    return pwd_context.hash(password)

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# --- 0. CONFIGURACIÓN DE ZONA HORARIA ECUADOR ---
ECUADOR_TZ = pytz.timezone('America/Guayaquil')  # UTC-5

def ahora_ecuador():
    return datetime.datetime.now(ECUADOR_TZ)


def crear_coleccion_rekognition():
    if not rekog:
        return
    try:
        rekog.describe_collection(CollectionId=COLLECTION_ID)
        print(f"✅ Colección {COLLECTION_ID} ya existe")
    except rekog.exceptions.ResourceNotFoundException:
        rekog.create_collection(CollectionId=COLLECTION_ID)
        print(f"✅ Colección {COLLECTION_ID} creada")
    except Exception as e:
        print(f"⚠️ Error con Rekognition: {e}")
# --- CONFIGURACIÓN DE CORREO ---
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 465  # <--- Asegúrate de que sea 465
SMTP_EMAIL = "karlos.ayala.lopez.1234@gmail.com"
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")

# --- 1. CONFIGURACIÓN Y CREDENCIALES AWS/B2 ---
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
AWS_REGION = "us-east-1"
COLLECTION_ID = "estudiantes_db"

# Inicialización condicional de AWS Rekognition
try:
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        rekog = boto3.client('rekognition', region_name=AWS_REGION, 
                           aws_access_key_id=AWS_ACCESS_KEY, 
                           aws_secret_access_key=AWS_SECRET_KEY)
        print("✅ AWS Rekognition inicializado")
    else:
        rekog = None
        print("⚠️ AWS Rekognition no disponible (credenciales faltantes)")
except Exception as e:
    rekog = None
    print(f"⚠️ Error inicializando AWS Rekognition: {e}")

# Configuración Backblaze B2 - usando variables de entorno (más seguro)
ENDPOINT_B2 = "https://s3.us-east-005.backblazeb2.com"
KEY_ID_B2 = os.environ.get("B2_KEY_ID", "005ae1e673cca820000000001")
APP_KEY_B2 = os.environ.get("B2_APP_KEY", "K005fAg5b+1v7qfwwfndGTvQsoS86Ms")
BUCKET_NAME = "despertar-evidencias-2026"  # <--- TU BUCKET REAL

try:
    my_config = Config(signature_version='s3v4', region_name='us-east-005')
    s3_client = boto3.client('s3', 
                            endpoint_url=ENDPOINT_B2,
                            aws_access_key_id=KEY_ID_B2,
                            aws_secret_access_key=APP_KEY_B2,
                            config=my_config)
    print("✅ Cliente S3 (Backblaze) inicializado")
except Exception as e:
    s3_client = None
    print(f"⚠️ Cliente S3 no disponible: {e}")

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def enviar_correo_real(destinatario: str, asunto: str, mensaje: str, html: bool = False) -> bool:
    import requests
    API_KEY = "re_8yGaLtUa_L4VewBKb1miDQcLAUe6jzQq6" 
    try:
        url = "https://api.resend.com/emails"
        payload = {
            "from": "onboarding@resend.dev",  # <--- CAMBIO TEMPORAL
            "to": [destinatario],
            "subject": asunto,
            "html": mensaje if html else f"<p>{mensaje}</p>"
        }
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code not in [200, 201]:
            logging.error(f"❌ Resend error: {response.status_code} - {response.text}")
        else:
            logging.info(f"✅ Correo enviado a {destinatario}")
        return response.status_code in [200, 201]
    except Exception as e:
        logging.error(f"❌ Excepción en correo: {e}")
        return False

# --- LÓGICA DE VOLUMEN PERSISTENTE ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILENAME = "Bases_de_datos.db"
VOLUMEN_PATH = "/app/datos_persistentes"

# Determinar ruta final de la base de datos
if os.path.exists(VOLUMEN_PATH):
    db_en_volumen = os.path.join(VOLUMEN_PATH, DB_FILENAME)
    if not os.path.exists(db_en_volumen):
        db_original = os.path.join(BASE_DIR, DB_FILENAME)
        if os.path.exists(db_original):
            shutil.copy(db_original, db_en_volumen)
            print(f"✅ Base de datos copiada al volumen persistente: {db_en_volumen}")
    DB_NAME = db_en_volumen
else:
    DB_NAME = os.path.join(BASE_DIR, DB_FILENAME)

print(f"📁 Ruta base de datos: {DB_NAME}")

def get_db_connection():
    try:
        import os
        conn = psycopg2.connect(
            host=os.environ.get("SUPABASE_HOST", "aws-1-us-east-1.pooler.supabase.com"),
            port=int(os.environ.get("SUPABASE_PORT", "6543")),
            user=os.environ.get("SUPABASE_USER", "postgres.udklgsmabwwfxpstmpxj"),
            password=os.environ.get("SUPABASE_PASSWORD", "Monte55or¡2021&"),
            dbname=os.environ.get("SUPABASE_DB", "postgres")
        )
        conn.cursor_factory = RealDictCursor
        print("✅ Conexión exitosa a Supabase.")
        return conn
    except Exception as e:
        print(f"❌ Error conectando a Supabase: {e}")
        return None
    
# --- FUNCIONES DE MANTENIMIENTO ---
def optimizar_sistema_db():
    """Ejecuta mantenimiento VACUUM en Supabase"""
    try:
        conn = get_db_connection()
        # En Postgres, VACUUM no puede ejecutarse dentro de una transacción
        conn.autocommit = True 
        with conn.cursor() as c:
            c.execute("VACUUM")
            c.execute("ANALYZE")
        conn.close()
        print("✅ Sistema optimizado (VACUUM ejecutado en Supabase)")
        return True
    except Exception as e:
        print(f"⚠️ Alerta menor: No se pudo optimizar DB: {e}")
        return False

class EstadoUsuarioRequest(BaseModel):
    cedula: str
    activo: int
    clave_maestra: Optional[str] = None # <--- ESTO ES LO QUE TE FALTA

class BackupRequest(BaseModel):
    tipo: str = "completo"

# --- 2. INICIALIZACIÓN DE BASE DE DATOS - MEJORADA ---
# --- INICIALIZACIÓN DE TABLAS ---
def init_db_completa():
    print("🔄 Iniciando configuración de base de datos en Supabase...")
    try:
        conn = get_db_connection()
        if not conn:
            print("❌ No hay conexión a BD, abortando init.")
            return
            
        c = conn.cursor()
        
        # 1. Tabla Usuarios
        c.execute('''CREATE TABLE IF NOT EXISTS Usuarios (
            ID SERIAL PRIMARY KEY,
            Nombre TEXT NOT NULL,
            Apellido TEXT NOT NULL,
            CI TEXT UNIQUE NOT NULL,
            Password TEXT NOT NULL,
            Tipo INTEGER DEFAULT 1,
            Foto TEXT,
            Activo INTEGER DEFAULT 1,
            Fecha_Desactivacion TIMESTAMP NULL,
            Ultimo_Acceso TIMESTAMP NULL,
            TutorialVisto INTEGER DEFAULT 0,
            Face_Encoding TEXT,
            Fecha_Registro TIMESTAMP DEFAULT NOW(),
            Email TEXT,
            Telefono TEXT
        )''')
        
        # 2. Tabla Evidencias
        c.execute('''CREATE TABLE IF NOT EXISTS Evidencias (
            id SERIAL PRIMARY KEY,
            CI_Estudiante TEXT NOT NULL,
            Url_Archivo TEXT NOT NULL,
            Hash TEXT NOT NULL,
            Estado INTEGER DEFAULT 1,
            Tipo_Archivo TEXT DEFAULT 'documento',
            Fecha TIMESTAMP DEFAULT NOW(),
            Tamanio_KB REAL DEFAULT 0,
            Asignado_Automaticamente INTEGER DEFAULT 0
        )''')

        # 3. Tabla Solicitudes
        c.execute('''CREATE TABLE IF NOT EXISTS Solicitudes (
            id SERIAL PRIMARY KEY,
            Tipo TEXT NOT NULL,
            CI_Solicitante TEXT NOT NULL,
            Email TEXT,
            Detalle TEXT,
            Evidencia_Reportada_Url TEXT,
            Id_Evidencia INTEGER,
            Resuelto_Por TEXT,
            Respuesta TEXT,
            Fecha TIMESTAMP DEFAULT NOW(),
            Estado TEXT DEFAULT 'PENDIENTE',
            Fecha_Resolucion TIMESTAMP NULL
        )''')
        
        # 4. Tabla Auditoria
        c.execute('''CREATE TABLE IF NOT EXISTS Auditoria (
            id SERIAL PRIMARY KEY,
            Accion TEXT NOT NULL,
            Detalle TEXT,
            IP TEXT,
            Usuario TEXT,
            Fecha TIMESTAMP DEFAULT NOW()
        )''')
        
        # 5. Tabla Métricas
        c.execute('''CREATE TABLE IF NOT EXISTS Metricas_Sistema (
            id SERIAL PRIMARY KEY,
            Fecha DATE UNIQUE,
            Total_Usuarios INTEGER DEFAULT 0,
            Total_Evidencias INTEGER DEFAULT 0,
            Solicitudes_Pendientes INTEGER DEFAULT 0,
            Almacenamiento_MB REAL DEFAULT 0
        )''')
        
        # Crear Admin
        c.execute("SELECT CI FROM Usuarios WHERE Tipo=0")
        if not c.fetchone():
            c.execute("INSERT INTO Usuarios (Nombre, Apellido, CI, Password, Tipo, Activo) VALUES (%s,%s,%s,%s,%s,%s)", 
          ('Admin', 'Sistema', '9999999999', get_password_hash('admin123'), 0, 1))
            print("✅ Usuario admin creado en Supabase")

        # Crear Bandeja Recuperados
        c.execute("SELECT CI FROM Usuarios WHERE CI='9999999990'")
        if not c.fetchone():
            c.execute("INSERT INTO Usuarios (Nombre, Apellido, CI, Password, Tipo, Activo, Foto) VALUES (%s,%s,%s,%s,%s,%s,%s)", 
                     ('Bandeja', 'Recuperados', '9999999990', '123456', 1, 1, ''))
            print("✅ Bandeja de Recuperados creada")
        
        conn.commit()
        conn.close()
        print("✅ Base de datos Supabase inicializada correctamente.")
        
    except Exception as e:
        print(f"❌ Error inicializando Supabase: {e}")

# EJECUTAR INICIALIZACIÓN (¡Ahora sí, al final de las definiciones!)
init_db_completa()

# =========================================================================
# 3. FUNCIONES AUXILIARES
# =========================================================================

def registrar_auditoria(accion: str, detalle: str, usuario: str = "Sistema", ip: str = ""):
    """
    Registra una acción OBLIGANDO la hora de Ecuador como TEXTO PLANO.
    Esto evita que la base de datos la convierta a UTC.
    """
    conn = None
    try:
        # 1. Obtener la hora exacta de Ecuador
        fecha_obj = ahora_ecuador()
        
        # 2. TRUCO DE ORO: Convertirla a texto simple AQUI en Python
        # Así la base de datos guarda "2026-01-01 10:00" tal cual, sin sumar 5 horas.
        fecha_str = fecha_obj.strftime("%Y-%m-%d %H:%M:%S")
        
        conn = get_db_connection()
        if conn:
            c = conn.cursor()
            c.execute("""
                INSERT INTO Auditoria (Accion, Detalle, Usuario, IP, Fecha) 
                VALUES (%s, %s, %s, %s, %s)
            """, (accion, detalle, usuario, ip, fecha_str)) # <--- Enviamos el texto, no el objeto
            
            conn.commit()
            print(f"📝 LOG REGISTRADO (EC): {accion} - {detalle}")
    except Exception as e:
        print(f"❌ Error en auditoria: {e}")
    finally:
        if conn: conn.close()
    
def calcular_hash(ruta: str) -> str:
    """Calcula hash SHA256 de un archivo"""
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()

def obtener_tamanio_archivo_kb(ruta: str) -> float:
    """Obtiene el tamaño de un archivo en KB"""
    try:
        return os.path.getsize(ruta) / 1024
    except:
        return 0

# --- REEMPLAZA TU FUNCIÓN 'identificar_rostro_aws' POR ESTA ---
def preparar_imagen_aws(ruta_imagen):
    """
    1. Lee la imagen (soporta AVIF si las librerías están instaladas).
    2. La convierte SIEMPRE a JPG (compatible con AWS).
    3. Comprime si pesa más de 5MB.
    """
    MAX_BYTES = 5242880 # 5 MB
    
    try:
        # Intentamos leer con OpenCV
        img = cv2.imread(ruta_imagen)
        
        # Si OpenCV falla (común con AVIF antiguos), intentamos con PIL (si está instalado)
        if img is None:
            try:
                from PIL import Image
                import numpy as np
                pil_img = Image.open(ruta_imagen).convert('RGB')
                img = np.array(pil_img) 
                # Convertir RGB (PIL) a BGR (OpenCV)
                img = img[:, :, ::-1].copy() 
            except ImportError:
                pass # Si no hay PIL, nos rendimos

        # Si después de todo no pudimos leer la imagen...
        if img is None:
            # Si es AVIF y no pudimos leerla, NO podemos mandarla cruda a AWS. Retornamos None o error.
            if ruta_imagen.lower().endswith('.avif'):
                print("⚠️ Error: No se pudo decodificar AVIF. Instala 'pillow-avif-plugin'.")
                return None
            # Si es JPG/PNG, mandamos crudo
            with open(ruta_imagen, 'rb') as f: return f.read()

        # COMPRESIÓN / CONVERSIÓN A JPG
        # Esto transforma el AVIF/WEBP/PNG a un JPG estándar que AWS sí entiende
        _, buffer = cv2.imencode('.jpg', img, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
        return buffer.tobytes()
        
    except Exception as e:
        print(f"⚠️ Error preparando imagen AWS: {e}")
        with open(ruta_imagen, 'rb') as f: return f.read()

def identificar_varios_rostros_aws(imagen_path: str, confidence_threshold: float = 70.0) -> List[str]:
    if not rekog: return []
    cedulas_encontradas = set()
    
    try:
        # 1. Cargar imagen con OpenCV (para recortes)
        img = cv2.imread(imagen_path)
        if img is None: return []
        height, width, _ = img.shape
        
        # 2. Obtener BYTES SEGUROS (Comprimidos si es necesario)
        image_bytes = preparar_imagen_aws(imagen_path)
            
        # 3. Detectar caras en la imagen (usando los bytes seguros)
        response_detect = rekog.detect_faces(Image={'Bytes': image_bytes})
        
        if not response_detect['FaceDetails']: return []

        # 4. Procesar cada cara
        for faceDetail in response_detect['FaceDetails']:
            bbox = faceDetail['BoundingBox']
            x = int(bbox['Left'] * width)
            y = int(bbox['Top'] * height)
            w = int(bbox['Width'] * width)
            h = int(bbox['Height'] * height)
            
            x, y = max(0, x), max(0, y)
            w, h = min(width - x, w), min(height - y, h)
            face_crop = img[y:y+h, x:x+w]
            if face_crop.size == 0: continue

            _, buffer = cv2.imencode('.jpg', face_crop)
            crop_bytes = buffer.tobytes()
            
            try:
                # AQUÍ ESTÁ EL CAMBIO CLAVE: MaxFaces=5 (Antes era 1)
                # Esto permite que si dos usuarios tienen la misma cara, AWS devuelva a ambos.
                search_res = rekog.search_faces_by_image(
                    CollectionId=COLLECTION_ID,
                    Image={'Bytes': crop_bytes},
                    MaxFaces=5, 
                    FaceMatchThreshold=confidence_threshold
                )
                
                # Ahora iteramos sobre TODAS las coincidencias encontradas para esa cara
                for match in search_res['FaceMatches']:
                    ced = match['Face'].get('ExternalImageId')
                    if ced: cedulas_encontradas.add(ced)
                    
            except: continue 

        return list(cedulas_encontradas)
    except Exception as e:
        print(f"Error IA Rostros: {e}")
        return []
    

# --- REEMPLAZA TU FUNCIÓN 'buscar_estudiantes_por_texto' POR ESTA VERSIÓN CON DEBUG VISUAL ---

def buscar_estudiantes_por_texto(imagen_path: str, cursor): # <--- Ahora recibe un cursor
    if not rekog: return [], []
    cedulas_encontradas = set()
    texto_leido_debug = []
    
    try:
        image_bytes = preparar_imagen_aws(imagen_path)
        response = rekog.detect_text(Image={'Bytes': image_bytes})
        
        palabras_sueltas = [t['DetectedText'].lower() for t in response.get('TextDetections', []) if t['Type'] == 'WORD']
        
        # --- USAMOS EL CURSOR 'cursor' RECIBIDO ---
        cursor.execute("SELECT Nombre, Apellido, CI FROM Usuarios WHERE Tipo=1")
        estudiantes = cursor.fetchall()
        
        for est in estudiantes:
            nombre_db = est.get('Nombre') or est.get('nombre')
            apellido_db = est.get('Apellido') or est.get('apellido')
            ci_db = est.get('CI') or est.get('ci')
            
            # Lógica de comparación...
            partes = nombre_db.lower().split() + apellido_db.lower().split()
            piezas_validas = {p for p in partes if len(p) > 2}
            
            coincidencias = 0
            for pieza in piezas_validas:
                if difflib.get_close_matches(pieza, palabras_sueltas, n=1, cutoff=0.8):
                    coincidencias += 1
            
            if coincidencias >= 2:
                cedulas_encontradas.add(ci_db)

    except Exception as e:
        print(f"⚠️ Error OCR: {e}")
        
    return list(cedulas_encontradas), texto_leido_debug

def coincidencia_difusa(partes_buscadas, palabras_en_imagen, umbral):
    """
    Verifica si TODAS las 'partes_buscadas' están presentes en 'palabras_en_imagen'
    con cierta tolerancia a errores (umbral).
    """
    aciertos = 0
    # Usamos una copia para no afectar búsquedas de otros estudiantes
    pool = palabras_en_imagen.copy()
    
    for parte in partes_buscadas:
        # Busca la palabra más parecida en la 'bolsa' de palabras de la imagen
        matches = difflib.get_close_matches(parte, pool, n=1, cutoff=umbral)
        if matches:
            aciertos += 1
            # Opcional: pool.remove(matches[0]) si quisieras evitar repetir palabras
            
    # Éxito si encontramos TODAS las partes (Ej: Encontró "Juan" Y encontró "Perez")
    return aciertos == len(partes_buscadas)

# Función auxiliar por si no la tienes
def calcular_hash(file_path):
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
    
def calcular_estadisticas_reales() -> dict:
    conn = None # <--- 1. Inicializar en None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # 1. Contar usuarios activos (Estudiantes Tipo 1 activos)
        c.execute("SELECT COUNT(*) as total FROM Usuarios WHERE Tipo = 1 AND Activo = 1")
        fila_usuarios = c.fetchone()
        usuarios_activos = fila_usuarios['total'] if fila_usuarios else 0
        
        # 2. Contar evidencias (SOLO DE ESTUDIANTES - JOIN CON USUARIOS)
        # Esto excluye automáticamente las fotos subidas al perfil del Admin
        c.execute("""
            SELECT COUNT(e.id) as total 
            FROM Evidencias e
            JOIN Usuarios u ON e.CI_Estudiante = u.CI
            WHERE u.Tipo = 1
        """)
        fila_evidencias = c.fetchone()
        total_evidencias = fila_evidencias['total'] if fila_evidencias else 0
        
        # 3. Sumar peso (SOLO DE ESTUDIANTES)
        c.execute("""
            SELECT SUM(e.Tamanio_KB) as peso_total 
            FROM Evidencias e
            JOIN Usuarios u ON e.CI_Estudiante = u.CI
            WHERE u.Tipo = 1
        """)
        fila_peso = c.fetchone()
        resultado_kb = fila_peso['peso_total'] if fila_peso and fila_peso['peso_total'] else 0
        
        # Lógica de estimación
        total_kb = resultado_kb
        nota_almacenamiento = "Calculado exacto (Solo Estudiantes)"
        
        # Corrección para cuando hay archivos pero pesan 0
        if total_kb == 0 and total_evidencias > 0:
            total_kb = total_evidencias * 2500 

        tamanio_total_mb = total_kb / 1024
        
        # Costos (Basados solo en consumo de estudiantes)
        costo_rekognition = (total_evidencias / 1000) * 1.0
        costo_almacenamiento = (tamanio_total_mb / 1024) * 0.023
        
        # 4. Solicitudes pendientes
        c.execute("SELECT COUNT(*) as total FROM Solicitudes WHERE Estado = 'PENDIENTE'")
        fila_solicitudes = c.fetchone()
        solicitudes_pendientes = fila_solicitudes['total'] if fila_solicitudes else 0
        
        return {
            "usuarios_activos": usuarios_activos,
            "total_evidencias": total_evidencias,
            "almacenamiento_mb": round(tamanio_total_mb, 2),
            "almacenamiento_gb": round(tamanio_total_mb / 1024, 4),
            "costo_estimado_usd": {
                "rekognition": round(costo_rekognition, 2),
                "almacenamiento": round(costo_almacenamiento, 4),
                "total": round(costo_rekognition + costo_almacenamiento, 2)
            },
            "solicitudes_pendientes": solicitudes_pendientes,
            "nota": nota_almacenamiento
        }
    except Exception as e:
        print(f"Error estadisticas: {e}")
        return {
            "usuarios_activos": 0, "total_evidencias": 0, 
            "almacenamiento_mb": 0, "almacenamiento_gb": 0,
            "solicitudes_pendientes": 0
        }
    finally: # <--- 3. AGREGAR EL CIERRE SEGURO
        if conn: conn.close()

# =========================================================================
# 4. CONFIGURACIÓN FASTAPI
# =========================================================================
app = FastAPI(title="Sistema Educativo Despertar", version="7.0")

# Lista de dominios permitidos para conectar con el backend
origins = [
    "https://www.uepdespertar-evidencias.work",
    "https://uepdespertar-evidencias.work",
    "https://proyecto-grado-karlos.vercel.app",
    "http://localhost:5500",
    "http://127.0.0.1:5500"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    print("\n" + "="*50)
    print("🚀 INICIANDO SERVIDOR - MODO PRODUCCIÓN")
    print("="*50)
    
    try:
        init_db_completa()
        print("✅ Base de datos verificada.")
        
        try:
            limpieza_duplicados_startup() 
        except Exception as e_limpieza:
            print(f"⚠️ Advertencia: La limpieza inicial falló: {e_limpieza}")

        # 👇 ESTO ES LO NUEVO
        crear_coleccion_rekognition()

    except Exception as e:
        print(f"❌ Error crítico en el inicio: {e}")

# =========================================================================
# 5. ENDPOINTS PRINCIPALES
# =========================================================================

@app.get("/")
def home():
    """Endpoint raíz del sistema"""
    return {
        "status": "online", 
        "backend": "Sistema Educativo Despertar V7.0",
        "cors_enabled": True,
        "zona_horaria": "America/Guayaquil (UTC-5)",
        "timestamp": ahora_ecuador().isoformat()
    }

@app.get("/health")
async def health_check():
    conn = get_db_connection()
    if conn is None:
        return JSONResponse(content={"status": "unhealthy", "error": "No se pudo conectar a la base de datos"})
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM Usuarios")
        usuarios = c.fetchone()['count']
        
        c.execute("SELECT COUNT(*) as count FROM Evidencias")
        evidencias = c.fetchone()['count']
        
        conn.close()
        
        return JSONResponse(content={
            "status": "healthy",
            "timestamp": ahora_ecuador().isoformat(),
            "database": "connected",
            "estadisticas": {
                "usuarios": usuarios,
                "evidencias": evidencias
            },
            "aws_rekognition": "available" if rekog else "unavailable",
            "s3_storage": "available" if s3_client else "unavailable"
        })
    except Exception as e:
        return JSONResponse(content={
            "status": "unhealthy",
            "error": str(e)
        })

# =========================================================================
# 6. ENDPOINTS DE AUTENTICACIÓN
# =========================================================================

class TemaRequest(BaseModel):
    cedula: str
    tema: int # 0 = Claro, 1 = Oscuro

@app.post("/actualizar_tema")
async def actualizar_tema(datos: TemaRequest):
    """Guarda si el usuario prefiere modo oscuro o claro"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("UPDATE Usuarios SET Tema = %s WHERE CI = %s", (datos.tema, datos.cedula))
        conn.commit()
        conn.close()
        return {"status": "ok", "mensaje": "Tema actualizado"}
    except Exception as e:
        return {"status": "error", "mensaje": str(e)}

@app.post("/iniciar_sesion")
async def iniciar_sesion(cedula: str = Form(...), contrasena: str = Form(...)):
    """Inicio de sesión corregido que incluye el TEMA del usuario"""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute("SELECT * FROM Usuarios WHERE CI = %s", (cedula.strip(),))
        u = c.fetchone()
        
        pass_db = u.get('password') or u.get('Password') if u else None
        
        if u and verify_password(contrasena.strip(), pass_db):
            # Log de auditoría
            nombre_completo = f"{u.get('Nombre') or u.get('nombre')} {u.get('Apellido') or u.get('apellido')}"
            rol_num = u.get('Tipo') if u.get('Tipo') is not None else u.get('tipo')
            rol_texto = "Administrador" if rol_num == 0 else "Estudiante"
            
            registrar_auditoria("INICIO_SESION", f"Ingreso exitoso ({rol_texto})", nombre_completo)
            
            # Datos para la página web
            datos_para_front = {
                "id": u.get('id') or u.get('ID'),
                "cedula": u.get('ci') or u.get('CI'),
                "nombre": u.get('nombre') or u.get('Nombre'),
                "apellido": u.get('apellido') or u.get('Apellido'),
                "url_foto": u.get('url_foto') or u.get('Url_Foto') or "",
                "email": u.get('email') or u.get('Email') or "",
                "tipo": rol_num,
                "tema": u.get('Tema') or u.get('tema') or 0, # <--- ¡AQUÍ VA EL TEMA!
                "tutorial_visto": u.get('tutorial_visto') or u.get('TutorialVisto') or 0
            }
            
            return JSONResponse({"autenticado": True, "datos": jsonable_encoder(datos_para_front)})
        
        
        else:
             # Contraseña incorrecta
          return JSONResponse({"autenticado": False, "mensaje": "Cédula o contraseña incorrectos."})
    except Exception as e:
        print(f"❌ Error login: {e}")
        return JSONResponse({"autenticado": False, "mensaje": str(e)})
    finally:
        if conn: conn.close()

@app.post("/buscar_estudiante")
async def buscar_estudiante(cedula: str = Form(...)):
    """
    Versión BLINDADA: Ordena por ID para evitar errores con nombres de fechas.
    """
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Buscar Datos Personales
        c.execute("SELECT * FROM Usuarios WHERE CI = %s", (cedula.strip(),))
        u = c.fetchone()
        
        if u:
            # Construir respuesta con todos los datos necesarios
            datos = {
                "id": u.get('id') or u.get('ID'),
                "cedula": u.get('ci') or u.get('CI'),
                "nombre": u.get('nombre') or u.get('Nombre'),
                "apellido": u.get('apellido') or u.get('Apellido'),
                # Buscamos la foto en todas sus posibles variantes de nombre
                "url_foto": u.get('url_foto') or u.get('Url_Foto') or u.get('Foto') or u.get('foto') or "",
                "email": u.get('email') or u.get('Email') or "",
                "tipo": u.get('tipo') or u.get('Tipo'),
                "tema": u.get('Tema') or u.get('tema') or 0,
                "galeria": [] 
            }
            
            # 2. Buscar Evidencias (CORRECCIÓN AQUÍ: Usamos 'ORDER BY id DESC')
            # El ID nunca falla. ID más alto = Foto más reciente.
            c.execute("SELECT * FROM Evidencias WHERE CI_Estudiante = %s ORDER BY id DESC", (cedula.strip(),))
            evidencias = c.fetchall()
            if evidencias:
                datos['galeria'] = [dict(row) for row in evidencias]

            return JSONResponse({"status": "ok", "encontrado": True, "datos": jsonable_encoder(datos)})
        
        return JSONResponse({"status": "error", "mensaje": "Usuario no encontrado"})
        
    except Exception as e:
        print(f"❌ Error crítico en buscar_estudiante: {e}")
        return JSONResponse({"status": "error", "mensaje": f"Error del servidor: {str(e)}"})
    finally:
        if conn: conn.close()
    
# =========================================================================
# 7. ENDPOINTS DE GESTIÓN DE USUARIOS
# =========================================================================

@app.post("/registrar_usuario")
async def registrar_usuario(
    nombre: str = Form(...),
    apellido: str = Form(...),
    cedula: str = Form(...),
    contrasena: str = Form(...),
    tipo_usuario: int = Form(...),
    foto: UploadFile = File(...)
):
    """
    Registra un nuevo usuario con zona horaria Ecuador.
    - Sube la foto a Backblaze B2 (S3) con ACL pública.
    - Guarda la URL en la base de datos.
    - Indexa el rostro en AWS Rekognition si es estudiante.
    """
    temp_dir = None
    conn = None
    try:
        import re
        
        # 1. Limpiar y validar datos
        cedula = cedula.strip()
        contrasena = contrasena.strip()
        nombre = nombre.strip()
        apellido = apellido.strip()
        
        if not cedula or not contrasena or not nombre or not apellido:
            return JSONResponse(
                content={"error": "Todos los campos son requeridos"},
                status_code=400
            )
        
        if len(cedula) != 10 or not cedula.isdigit():
            return JSONResponse(
                content={"error": "La cédula debe tener 10 dígitos numéricos"},
                status_code=400
            )
        
        if len(contrasena) < 6:
            return JSONResponse(
                content={"error": "La contraseña debe tener al menos 6 caracteres"},
                status_code=400
            )
        
        # 2. Conectar a la base de datos
        conn = get_db_connection()
        if not conn:
            return JSONResponse(
                content={"error": "Error de conexión a la base de datos"},
                status_code=500
            )
        
        c = conn.cursor()
        
        # 3. Verificar si el usuario ya existe
        c.execute("SELECT CI FROM Usuarios WHERE CI = %s", (cedula,))
        if c.fetchone():
            conn.close()
            return JSONResponse(
                content={"error": "Usuario ya existe en el sistema"},
                status_code=409
            )
        
        # 4. Sanitizar nombre del archivo
        nombre_limpio = re.sub(r'[^\w\.\-]', '_', foto.filename)
        if not nombre_limpio:
            nombre_limpio = "foto_perfil.jpg"
        
        # 5. Guardar archivo temporalmente
        temp_dir = tempfile.mkdtemp()
        foto_path = os.path.join(temp_dir, nombre_limpio)
        with open(foto_path, "wb") as f:
            shutil.copyfileobj(foto.file, f)
        
        # 6. Subir a Backblaze B2 (S3) - ¡OBLIGATORIO!
        if not s3_client:
            conn.close()
            return JSONResponse(
                content={"error": "Almacenamiento en nube no disponible"},
                status_code=500
            )
        
        timestamp = int(ahora_ecuador().timestamp())
        nombre_nube = f"perfiles/{cedula}_{timestamp}_{nombre_limpio}"
        url_foto = ""
        
        try:
            s3_client.upload_file(
                foto_path,
                BUCKET_NAME,
                nombre_nube,
                ExtraArgs={'ACL': 'public-read'}
            )
            url_foto = f"https://{BUCKET_NAME}.s3.us-east-005.backblazeb2.com/{nombre_nube}"
            print(f"✅ Foto subida a S3: {url_foto}")
        except Exception as e_s3:
            conn.close()
            print(f"❌ Error subiendo a S3: {e_s3}")
            return JSONResponse(
                content={"error": f"Error al subir la foto a la nube: {str(e_s3)}"},
                status_code=502
            )
        
        # 7. Encriptar contraseña
        hashed_password = get_password_hash(contrasena)
        
        # 8. Insertar usuario en la base de datos
        fecha_str = ahora_ecuador().strftime("%Y-%m-%d %H:%M:%S")
        
        c.execute("""
            INSERT INTO Usuarios 
            (Nombre, Apellido, CI, Password, Tipo, Foto, Activo, Fecha_Registro)
            VALUES (%s, %s, %s, %s, %s, %s, 1, %s)
        """, (
            nombre,
            apellido,
            cedula,
            hashed_password,
            tipo_usuario,
            url_foto,
            fecha_str
        ))
        
        # 9. Indexar rostro en AWS Rekognition (solo si es estudiante)
        if tipo_usuario == 1 and rekog:
            try:
                with open(foto_path, 'rb') as image_file:
                    image_bytes = image_file.read()
                
                rekog.index_faces(
                    CollectionId=COLLECTION_ID,
                    Image={'Bytes': image_bytes},
                    ExternalImageId=cedula,
                    MaxFaces=1,
                    QualityFilter='AUTO'
                )
                print(f"✅ Rostro indexado en AWS para estudiante {cedula}")
            except Exception as e_rek:
                print(f"⚠️ Error indexando rostro en AWS: {e_rek}")
                # No detenemos el proceso, solo advertimos
        
        conn.commit()
        conn.close()
        
        # 10. Limpiar archivos temporales
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        
        # 11. Registrar auditoría
        tipo_txt = "Administrador" if int(tipo_usuario) == 0 else "Estudiante"
        registrar_auditoria(
            "REGISTRO_USUARIO",
            f"El Admin creó al usuario: {nombre} {apellido} (CI: {cedula}) como {tipo_txt}",
            "Administrador"
        )
        
        # 12. Respuesta de éxito
        return JSONResponse(content={
            "status": "ok",
            "mensaje": f"Usuario {nombre} {apellido} registrado correctamente.",
            "url_foto": url_foto
        })
    
    except Exception as e:
        print(f"❌ Error en registrar_usuario: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            content={"error": f"Error interno del servidor: {str(e)}"},
            status_code=500
        )
    
    finally:
        # Asegurar limpieza de recursos
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if conn:
            conn.close()
    
@app.post("/cambiar_estado_usuario")
async def cambiar_estado_usuario(datos: EstadoUsuarioRequest):
    """Activa/desactiva usuario. Protege a Admins con Clave Maestra."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor) 
        
        # 1. Verificar qué tipo de usuario es el objetivo
        c.execute("SELECT Tipo, Nombre, Apellido FROM Usuarios WHERE CI = %s", (datos.cedula,))
        target_user = c.fetchone()
        
        if not target_user:
            return JSONResponse(content={"error": "Usuario no encontrado"}, status_code=404)

        # --- CORRECCIÓN CLAVE AQUÍ: Usamos .get() para leer sin importar mayúsculas/minúsculas ---
        tipo_usuario = target_user.get('Tipo') if target_user.get('Tipo') is not None else target_user.get('tipo')
        
        # 2. LÓGICA DE PROTECCIÓN DE ADMINS
        # Si intentan desactivar (activo=0) a un Admin (Tipo=0)
        if tipo_usuario == 0 and datos.activo == 0:
            # Verificamos la clave maestra
            if not datos.clave_maestra or datos.clave_maestra != CLAVE_SUPREMA:
                return JSONResponse(content={"error": "⛔ Acceso Denegado: Se requiere la Clave Suprema para desactivar a un Administrador."}, status_code=403)

        # 3. Proceder con el cambio
        fecha_desactivacion = ahora_ecuador() if datos.activo == 0 else None
        
        c.execute("""
            UPDATE Usuarios 
            SET Activo = %s, Fecha_Desactivacion = %s
            WHERE CI = %s
            RETURNING Nombre, Apellido
        """, (datos.activo, fecha_desactivacion, datos.cedula))
        
        user = c.fetchone()
        conn.commit()
        
        # Leemos el resultado con seguridad también
        u_nombre = user.get('Nombre') or user.get('nombre')
        u_apellido = user.get('Apellido') or user.get('apellido')
        
        nombre_completo = f"{u_nombre} {u_apellido}"
        estado_texto = "activada" if datos.activo == 1 else "desactivada"
        registrar_auditoria("CAMBIO_ESTADO", f"Cuenta de {nombre_completo} {estado_texto}", "Admin")
        
        return JSONResponse(content={"mensaje": "OK", "nombre": nombre_completo})
        
    except Exception as e:
        if conn: conn.rollback()
        # Imprimimos el error en la consola del servidor para que sepas qué pasó
        print(f"❌ Error cambiando estado: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)
    finally:
        if conn: conn.close()


@app.delete("/eliminar_usuario/{cedula}")
async def eliminar_usuario(
    cedula: str, 
    admin_cedula: str = Form(...), 
    admin_pass: str = Form(...) # <--- 1. AQUI PEDIMOS LA CONTRASEÑA
):
    conn = None
    try:  
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 2. Verificar credenciales del administrador
        c.execute("SELECT Tipo, Password FROM Usuarios WHERE CI = %s", (admin_cedula,))
        admin = c.fetchone()
    
        # Verificamos: Que exista, que sea Admin (0), y QUE LA CONTRASEÑA COINCIDA
        if not admin or admin['Tipo'] != 0 or not verify_password(admin_pass, admin['Password']):
            return JSONResponse({"error": "Credenciales de administrador inválidas o sin permisos"}, status_code=403)
        
        # --- A PARTIR DE AQUI TODO SIGUE IGUAL ---
        
        # 3. Obtener evidencias ANTES de intentar borrarlas
        c.execute("SELECT Url_Archivo FROM Evidencias WHERE CI_Estudiante = %s", (cedula,))
        evidencias = c.fetchall()

        # 4. Borrar archivos de evidencias en la nube (B2)
        if s3_client and BUCKET_NAME:
            for ev in evidencias:
                url = ev.get('Url_Archivo') or ev.get('url_archivo')
                if url and "backblazeb2.com" in url:
                    try:
                        partes = url.split(f"/file/{BUCKET_NAME}/")
                        if len(partes) > 1:
                            s3_client.delete_object(Bucket=BUCKET_NAME, Key=partes[1])
                    except Exception as e:
                        print(f"⚠️ No se pudo borrar archivo B2: {e}")

        # 5. Borrar registros de evidencias en BD
        c.execute("DELETE FROM Evidencias WHERE CI_Estudiante = %s", (cedula,))
        
        # 6. Obtener y borrar foto de perfil (Nube)
        c.execute("SELECT Foto FROM Usuarios WHERE CI = %s", (cedula,))
        usuario = c.fetchone()
        
        if usuario:
            url_foto = usuario.get('Foto') or usuario.get('foto')
            if url_foto and s3_client and BUCKET_NAME and "backblazeb2.com" in url_foto:
                try:
                    partes = url_foto.split(f"/file/{BUCKET_NAME}/")
                    if len(partes) > 1:
                        s3_client.delete_object(Bucket=BUCKET_NAME, Key=partes[1])
                except Exception as e:
                      print(f"⚠️ No se pudo borrar foto perfil B2: {e}")

        # 7. Finalmente borrar el usuario
        c.execute("DELETE FROM Usuarios WHERE CI = %s", (cedula,))
        
        conn.commit()
        return JSONResponse({"mensaje": "Usuario y todos sus datos eliminados correctamente"})
        
    except Exception as e: 
        print(f"❌ Error eliminando usuario completo: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    finally: # <--- AGREGUE ESTO TAMBIEN POR SEGURIDAD
        if conn: conn.close()
    
# =========================================================================
# 8. ENDPOINTS DE EVIDENCIAS
# =========================================================================

def garantizar_limite_storage(ruta_archivo, limite_mb=1000):
    """
    V3.0 - Optimización TOTAL:
    - Imágenes: Se redimensionan a Full HD (max 1600px) y calidad web (85%).
    - Videos: Se comprimen si pesan más de 1GB.
    """
    try:
        peso_actual_mb = os.path.getsize(ruta_archivo) / (1024 * 1024)
        ext = os.path.splitext(ruta_archivo)[1].lower()
        
        # --- 1. OPTIMIZACIÓN DE IMÁGENES (CRÍTICO PARA LIGHTHOUSE) ---
        if ext in ['.jpg', '.jpeg', '.png', '.webp', '.bmp']:
            try:
                # Leer imagen
                img = cv2.imread(ruta_archivo)
                if img is None: return ruta_archivo # Si falla, devolver original

                height, width = img.shape[:2]
                max_dimension = 1600 # 1600px es perfecto para web (buena calidad, poco peso)
                
                # Solo redimensionar si es más grande que el límite
                if width > max_dimension or height > max_dimension:
                    scale = max_dimension / max(width, height)
                    new_w = int(width * scale)
                    new_h = int(height * scale)
                    img = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)
                    
                    # Guardar imagen optimizada
                    dir_name = os.path.dirname(ruta_archivo)
                    base_name = os.path.splitext(os.path.basename(ruta_archivo))[0] + "_web.jpg"
                    ruta_optimizada = os.path.join(dir_name, base_name)
                    
                    # Compresión JPG Calidad 85
                    cv2.imwrite(ruta_optimizada, img, [int(cv2.IMWRITE_JPEG_QUALITY), 85])
                    
                    if os.path.exists(ruta_optimizada):
                        # Reemplazamos: borramos la original pesada y devolvemos la nueva
                        try: os.remove(ruta_archivo)
                        except: pass
                        print(f"✅ Imagen optimizada: {base_name}")
                        return ruta_optimizada
                
                return ruta_archivo # Si era pequeña, se devuelve tal cual

            except Exception as e_img:
                print(f"⚠️ Error optimizando imagen (se usará original): {e_img}")
                return ruta_archivo

        # --- 2. OPTIMIZACIÓN DE VIDEOS (SOLO SI SON GIGANTES) ---
        if peso_actual_mb <= limite_mb:
            return ruta_archivo 
            
        print(f"⚠️ Video gigante ({peso_actual_mb:.2f} MB). Comprimiendo...")
        dir_name = os.path.dirname(ruta_archivo)
        base_name = os.path.basename(ruta_archivo)
        ruta_comprimida = os.path.join(dir_name, f"compressed_{base_name}")
        
        if ext in ['.mp4', '.avi', '.mov', '.mkv']:
            cap = cv2.VideoCapture(ruta_archivo)
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
            scale = 0.5 if width > 1920 else 0.7
            new_w, new_h = int(width * scale), int(height * scale)
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            out = cv2.VideoWriter(ruta_comprimida, fourcc, 24, (new_w, new_h))
            while True:
                ret, frame = cap.read()
                if not ret: break
                frame_b = cv2.resize(frame, (new_w, new_h))
                out.write(frame_b)
            cap.release()
            out.release()
            
            if os.path.exists(ruta_comprimida):
                shutil.move(ruta_comprimida, ruta_archivo)
                return ruta_archivo
                
    except Exception as e:
        print(f"⚠️ Error general en optimización: {e}")
    
    return ruta_archivo

@app.post("/subir_evidencia_ia")
async def subir_evidencia_ia(archivo: UploadFile = File(...)):
    temp_dir = None
    conn = None
    try:
        # ------------------------------------------------------------
        # 1. SANITIZAR NOMBRE DE ARCHIVO (¡CLAVE PARA EVITAR ERROR 500!)
        # ------------------------------------------------------------
        # Elimina caracteres problemáticos: espacios, paréntesis, acentos, etc.
        import re
        nombre_original = archivo.filename
        nombre_limpio = re.sub(r'[^\w\.\-]', '_', nombre_original)  # Solo letras, números, puntos y guiones
        print(f"📥 Procesando: {nombre_original} -> {nombre_limpio}")

        # Crear directorio temporal y guardar con nombre limpio
        temp_dir = tempfile.mkdtemp()
        path = os.path.join(temp_dir, nombre_limpio)
        with open(path, "wb") as f:
            shutil.copyfileobj(archivo.file, f)
        print(f"   ✅ Archivo guardado en: {path}")

        # ------------------------------------------------------------
        # 2. CALCULAR HASH DEL ARCHIVO
        # ------------------------------------------------------------
        file_hash = calcular_hash(path)
        print(f"   🔑 Hash: {file_hash[:16]}...")

        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")
        c = conn.cursor(cursor_factory=RealDictCursor)

        # ------------------------------------------------------------
        # 3. DETERMINAR TIPO DE ARCHIVO
        # ------------------------------------------------------------
        ext = os.path.splitext(nombre_limpio)[1].lower()
        es_imagen = ext in ['.jpg', '.jpeg', '.png', '.bmp', '.webp', '.avif']
        es_video = ext in ['.mp4', '.avi', '.mov', '.mkv', '.webm']
        tipo_archivo = "video" if es_video else ("imagen" if es_imagen else "documento")
        print(f"   📂 Tipo: {tipo_archivo}")

        # ------------------------------------------------------------
        # 4. RECONOCIMIENTO CON IA (Rostros + OCR) - MANEJO DE ERRORES
        # ------------------------------------------------------------
        cedulas_detectadas = set()
        
        if rekog:
            try:
                if es_imagen:
                    print("   👤 Detectando rostros...")
                    try:
                        rostros = identificar_varios_rostros_aws(path)
                        print(f"   👥 Rostros encontrados: {rostros}")
                        cedulas_detectadas.update(rostros)
                    except Exception as e_rostro:
                        print(f"   ⚠️ Error en detección de rostros: {e_rostro}")
                    
                    print("   📝 Detectando texto (OCR)...")
                    try:
                        textos_ceds, _ = buscar_estudiantes_por_texto(path, c)
                        print(f"   📄 Cédulas por texto: {textos_ceds}")
                        cedulas_detectadas.update(textos_ceds)
                    except Exception as e_texto:
                        print(f"   ⚠️ Error en OCR: {e_texto}")

                elif es_video:
                    print("   🎬 Procesando video (fotogramas)...")
                    cap = cv2.VideoCapture(path)
                    frame_count = 0
                    while cap.isOpened():
                        ret, frame = cap.read()
                        if not ret or frame_count > 1800:
                            break
                        if frame_count % 60 == 0:
                            with tempfile.NamedTemporaryFile(suffix=".jpg", delete=False) as tmp:
                                _, buffer = cv2.imencode('.jpg', frame)
                                tmp.write(buffer.tobytes())
                                tmp_path = tmp.name
                            try:
                                rostros_f = identificar_varios_rostros_aws(tmp_path)
                                cedulas_detectadas.update(rostros_f)
                            except Exception as e_frame:
                                print(f"   ⚠️ Error en fotograma {frame_count}: {e_frame}")
                            if os.path.exists(tmp_path):
                                os.remove(tmp_path)
                        frame_count += 1
                    cap.release()
            except Exception as e_ia:
                print(f"   ❌ Error general en procesamiento IA: {e_ia}")
                # No detenemos el proceso, continuamos para al menos guardar el archivo.

        # ------------------------------------------------------------
        # 5. VERIFICAR SI EL ARCHIVO YA EXISTE (por Hash)
        # ------------------------------------------------------------
        c.execute("SELECT Url_Archivo, Tamanio_KB FROM Evidencias WHERE Hash = %s LIMIT 1", (file_hash,))
        evidencia_existente = c.fetchone()
        
        url_final = ""
        tamanio_kb = 0
        
        if evidencia_existente:
            url_final = evidencia_existente.get('Url_Archivo') or evidencia_existente.get('url_archivo')
            tamanio_kb = evidencia_existente.get('Tamanio_KB') or evidencia_existente.get('tamanio_kb') or 0
            print(f"   ♻️ Archivo ya existente. Reutilizando URL: {url_final}")
        else:
            # ------------------------------------------------------------
            # 6. SUBIR A LA NUBE (S3/Backblaze) - CON MANEJO DE ERRORES
            # ------------------------------------------------------------
            if not s3_client:
                raise HTTPException(status_code=500, detail="Error crítico: No hay conexión con el almacenamiento en la nube.")

            # Optimizar imagen/video antes de subir
            path_procesado = garantizar_limite_storage(path)
            tamanio_kb = os.path.getsize(path_procesado) / 1024
            
            # Generar nombre único en la nube (usando nombre limpio)
            timestamp = int(ahora_ecuador().timestamp())
            nombre_nube = f"evidencias/{timestamp}_{nombre_limpio}"
            
            try:
                s3_client.upload_file(
                    path_procesado,
                    BUCKET_NAME,
                    nombre_nube,
                    ExtraArgs={'ACL': 'public-read'}
                )
                url_final = f"https://{BUCKET_NAME}.s3.us-east-005.backblazeb2.com/{nombre_nube}"
                print(f"   ✅ Archivo subido exitosamente a S3: {url_final}")
            except Exception as e_s3:
                print(f"   ❌ Error subiendo a S3: {e_s3}")
                raise HTTPException(status_code=502, detail=f"Fallo al subir a la nube: {str(e_s3)}")

        # ------------------------------------------------------------
        # 7. ASIGNACIÓN INTELIGENTE
        # ------------------------------------------------------------
        asignados_nuevos = []
        ya_tenian = []
        
        if cedulas_detectadas:
            print(f"   🎯 Cedulas detectadas: {cedulas_detectadas}")
            for ced in cedulas_detectadas:
                c.execute("SELECT Nombre, Apellido, Tipo FROM Usuarios WHERE CI=%s", (ced,))
                u = c.fetchone()
                
                if u and (u.get('Tipo') or u.get('tipo')) == 1:
                    nombre_completo = f"{u.get('Nombre') or u.get('nombre')} {u.get('Apellido') or u.get('apellido')}"
                    
                    # Verificar si este estudiante ya tiene esta evidencia
                    c.execute("SELECT id FROM Evidencias WHERE CI_Estudiante = %s AND Hash = %s", (ced, file_hash))
                    if c.fetchone():
                        ya_tenian.append(nombre_completo)
                    else:
                        c.execute("""
                            INSERT INTO Evidencias 
                            (CI_Estudiante, Url_Archivo, Hash, Estado, Tipo_Archivo, Tamanio_KB, Asignado_Automaticamente) 
                            VALUES (%s, %s, %s, 1, %s, %s, 1)
                        """, (ced, url_final, file_hash, tipo_archivo, tamanio_kb))
                        asignados_nuevos.append(nombre_completo)

            # Construir mensaje de respuesta
            msg_parts = []
            if asignados_nuevos:
                msg_parts.append(f"✅ Evidencia asignada correctamente a: {', '.join(asignados_nuevos)}.")
            if ya_tenian:
                msg_parts.append(f"ℹ️ Omitiendo a: {', '.join(ya_tenian)} (ya la tenían en su perfil).")
            
            if not asignados_nuevos and ya_tenian:
                msg = f"⚠️ Todos los usuarios detectados ({', '.join(ya_tenian)}) ya contaban con esta evidencia."
                status = "alerta"
            else:
                msg = " ".join(msg_parts)
                status = "exito"
        else:
            # ------------------------------------------------------------
            # 8. SIN DETECCIÓN -> GUARDAR EN "PENDIENTES"
            # ------------------------------------------------------------
            print("   ⚠️ No se detectaron rostros ni texto. Guardando en Pendientes.")
            c.execute("SELECT id FROM Evidencias WHERE Hash = %s AND CI_Estudiante = 'PENDIENTE'", (file_hash,))
            if not c.fetchone():
                c.execute("""
                    INSERT INTO Evidencias 
                    (CI_Estudiante, Url_Archivo, Hash, Estado, Tipo_Archivo, Tamanio_KB, Asignado_Automaticamente) 
                    VALUES ('PENDIENTE', %s, %s, 1, %s, %s, 0)
                """, (url_final, file_hash, tipo_archivo, tamanio_kb))
                msg = "⚠️ No se identificó a nadie. Guardado en 'Pendientes'."
                status = "alerta"
            else:
                msg = "⚠️ El archivo ya se encuentra en la bandeja de 'Pendientes'."
                status = "alerta"

            # Registrar auditoría
            registrar_auditoria(
                "SUBIDA_IA_PENDIENTE",
                f"Archivo '{nombre_original}' enviado a Pendientes (Sin rostro/Duplicado)",
                "Sistema IA"
            )

        # ------------------------------------------------------------
        # 9. CONFIRMAR Y LIMPIAR
        # ------------------------------------------------------------
        conn.commit()
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        
        return JSONResponse({"status": status, "mensaje": msg})

    except HTTPException as http_exc:
        # Excepciones lanzadas intencionalmente (ej: error de S3)
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        print(f"❌ Error controlado: {http_exc.detail}")
        return JSONResponse({"status": "error", "mensaje": http_exc.detail}, status_code=http_exc.status_code)

    except Exception as e:
        # Cualquier otro error inesperado
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        print(f"❌ Error IA inesperado: {e}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            {"status": "error", "mensaje": f"Error interno del servidor: {str(e)}"},
            status_code=500
        )

    finally:
        if conn:
            conn.close()
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

@app.post("/subir_manual")
async def subir_manual(
    cedulas: str = Form(...), 
    archivo: UploadFile = File(...)
):
    """
    Sube una evidencia manualmente.
    CORRECCIÓN: Si el archivo ya existe (asignado a otro alumno), 
    RECICLA la URL y asigna el permiso al nuevo alumno sin dar error de duplicado.
    """
    temp_dir = None
    conn = None
    try:
        # 1. Validar que existan cédulas
        lista_cedulas = [c.strip() for c in cedulas.split(",") if c.strip()]
        if not lista_cedulas:
            return JSONResponse(content={"status": "error", "mensaje": "Debe especificar al menos una cédula"})
        
        # 2. Guardar archivo temporalmente para calcular Hash
        temp_dir = tempfile.mkdtemp()
        path = os.path.join(temp_dir, archivo.filename)
        with open(path, "wb") as f:
            shutil.copyfileobj(archivo.file, f)
        
        file_hash = calcular_hash(path)
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor) 
        
        # --- LÓGICA INTELIGENTE DE REUTILIZACIÓN ---
        # 3. Verificar si el archivo ya existe físicamente en el sistema
        c.execute("SELECT Url_Archivo, Tamanio_KB FROM Evidencias WHERE Hash = %s LIMIT 1", (file_hash,))
        evidencia_existente = c.fetchone()

        url_final = ""
        tamanio_kb = 0
        reutilizado = False

        if evidencia_existente:
            # CASO A: El archivo YA EXISTE (Ej: Lo tiene el estudiante A)
            # No lo subimos de nuevo. Reutilizamos la URL y el peso.
            url_final = evidencia_existente.get('Url_Archivo') or evidencia_existente.get('url_archivo')
            tamanio_kb = evidencia_existente.get('Tamanio_KB') or evidencia_existente.get('tamanio_kb') or 0
            reutilizado = True
            print(f"♻️ Archivo existente detectado. Reutilizando URL: {url_final}")
        else:
            # CASO B: El archivo es NUEVO. Lo subimos a la nube.
            path_procesado = garantizar_limite_storage(path)
            tamanio_kb = os.path.getsize(path_procesado) / 1024
            url_final = f"/local/{archivo.filename}"
            
            if s3_client:
                try:
                    nombre_nube = f"evidencias/manual_{int(ahora_ecuador().timestamp())}_{archivo.filename}"
                    s3_client.upload_file(path_procesado, BUCKET_NAME, nombre_nube, ExtraArgs={'ACL': 'public-read'})
                    url_final = f"https://{BUCKET_NAME}.s3.us-east-005.backblazeb2.com/{nombre_nube}"
                except Exception as e_upload:
                    print(f"⚠️ Error subiendo a S3: {e_upload}")
            
        # 4. Asignar a cada estudiante de la lista (Verificando que ESE estudiante no lo tenga ya)
        count = 0
        omitidos = 0
        
        # Detectar tipo archivo
        ext = os.path.splitext(archivo.filename)[1].lower()
        es_video = ext in ['.mp4', '.avi', '.mov', '.mkv']
        tipo_archivo = "video" if es_video else "imagen"
        if ext in ['.pdf', '.doc', '.docx']: tipo_archivo = "documento"

        for ced in lista_cedulas:
            # Verificamos si ESTE estudiante ya tiene ESTE archivo
            c.execute("SELECT id FROM Evidencias WHERE CI_Estudiante=%s AND Hash=%s", (ced, file_hash))
            ya_lo_tiene = c.fetchone()

            if not ya_lo_tiene:
                # Verificamos que el estudiante exista
                c.execute("SELECT CI FROM Usuarios WHERE CI=%s", (ced,))
                if c.fetchone():
                    c.execute("""
                        INSERT INTO Evidencias (CI_Estudiante, Url_Archivo, Hash, Estado, Tipo_Archivo, Tamanio_KB, Asignado_Automaticamente)
                        VALUES (%s, %s, %s, 1, %s, %s, 0)
                    """, (ced, url_final, file_hash, tipo_archivo, tamanio_kb))
                    count += 1
            else:
                omitidos += 1
        
        conn.commit()
        
        # Mensaje de respuesta
        if count > 0:
            msg = f"✅ Éxito: Archivo asignado a {count} estudiante(s)."
            if reutilizado: msg += " (Archivo reutilizado sin resubir)."
            registrar_auditoria("SUBIDA_MANUAL", f"Admin asignó '{archivo.filename}' a {count} usuarios.", "Administrador")
            return JSONResponse({"status": "ok", "mensaje": msg})
        else:
            if omitidos > 0:
                return JSONResponse({"status": "alerta", "mensaje": f"⚠️ Los estudiantes seleccionados ya tenían este archivo asignado."})
            else:
                return JSONResponse({"status": "error", "mensaje": "No se pudo asignar a ningún estudiante válido."})

    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ Error en subir_manual: {e}")
        return JSONResponse({"status": "error", "mensaje": str(e)})
    finally:
        if conn: conn.close()
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
    
# =========================================================================
# 9. ENDPOINTS DE BACKUP Y MANTENIMIENTO
# =========================================================================

@app.get("/crear_backup")
async def crear_backup():
    """Genera un archivo SQL con los datos REALES de Supabase"""
    try:
        conn = get_db_connection()
        
        # 🔥 ESTA ES LA LÍNEA MÁGICA QUE FALTABA
        # Desactivamos el "Modo Diccionario" para que nos de los valores reales (tuplas)
        conn.cursor_factory = None 
        
        c = conn.cursor()
        
        # Buffer en memoria
        output = io.StringIO()
        output.write(f"-- RESPALDO SISTEMA DESPERTAR --\n")
        output.write(f"-- FECHA: {ahora_ecuador()} --\n\n")
        
        tablas = ['Usuarios', 'Evidencias', 'Solicitudes', 'Auditoria', 'Metricas_Sistema']
        
        for tabla in tablas:
            try:
                # 1. Obtener datos
                c.execute(f"SELECT * FROM {tabla}")
                filas = c.fetchall()
                
                # 2. Obtener nombres de columnas
                if c.description:
                    columnas = [desc[0] for desc in c.description]
                    cols_str = ", ".join(columnas)
                    
                    output.write(f"\n-- DATA TABLE: {tabla} --\n")
                    
                    for fila in filas:
                        vals = []
                        # Ahora 'fila' es una lista de valores reales ('Juan', 'Perez'...), no de llaves.
                        for val in fila: 
                            if val is None:
                                vals.append("NULL")
                            elif isinstance(val, (int, float)):
                                vals.append(str(val))
                            elif isinstance(val, bool):
                                vals.append("TRUE" if val else "FALSE")
                            else:
                                # Escapar comillas simples para SQL
                                clean_val = str(val).replace("'", "''")
                                vals.append(f"'{clean_val}'")
                        
                        vals_str = ", ".join(vals)
                        output.write(f"INSERT INTO {tabla} ({cols_str}) VALUES ({vals_str});\n")
            except Exception as e_tab:
                output.write(f"-- Error exportando tabla {tabla}: {e_tab} --\n")

        conn.close()
        
        # Preparar descarga
        fecha_str = ahora_ecuador().strftime("%Y%m%d_%H%M")
        nombre_archivo = f"backup_completo_{fecha_str}.sql"
        
        mem_bytes = io.BytesIO(output.getvalue().encode('utf-8'))
        output.close()
        mem_bytes.seek(0)
        
        return StreamingResponse(
            mem_bytes,
            media_type="application/sql",
            headers={"Content-Disposition": f"attachment; filename={nombre_archivo}"}
        )
        
    except Exception as e:
        print(f"❌ Error backup SQL: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/descargar_multimedia_zip")
async def descargar_multimedia_zip():
    """
    Descarga evidencias S3 en un ZIP.
    V3.0 - FINAL: Sin reportes de texto y con nombres de archivo corregidos 
    para evitar errores de 'archivo dañado' en Windows/Word.
    """
    try:
        from urllib.parse import unquote
        import re # Importante para limpiar los nombres

        if not s3_client or not BUCKET_NAME:
            return JSONResponse({"error": "S3 no configurado"}, status_code=500)

        # 1. ESPIAR EL BUCKET (Mapa de Realidad)
        mapa_nube = {} 
        try:
            print("🕵️ Escaneando bucket real para descarga limpia...")
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=BUCKET_NAME):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        full_key = obj['Key']
                        # Guardamos el nombre limpio para buscarlo después
                        nombre_limpio = full_key.split('/')[-1]
                        mapa_nube[nombre_limpio] = full_key
        except Exception as e_scan:
            return JSONResponse({"error": f"Error escaneando bucket: {str(e_scan)}"}, status_code=500)

        # 2. OBTENER DATOS DE LA BD
        conn = get_db_connection()
        conn.cursor_factory = None 
        c = conn.cursor()
        
        # Truco para leer rápido y luego cerrar
        conn.close()
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute("SELECT * FROM Evidencias")
        evidencias = c.fetchall()
        conn.close()

        if not evidencias:
            return JSONResponse({"error": "Tabla Evidencias vacía"}, status_code=404)

        # 3. EMPAREJAR Y DESCARGAR (SIN LOGS DE TEXTO)
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            
            for ev in evidencias:
                url = ev.get('Url_Archivo') or ev.get('url_archivo') or ""
                id_ev = ev.get('id') or ev.get('ID')
                
                if not url: continue

                # Nombre original de la BD
                nombre_buscado = url.split('/')[-1]
                nombre_buscado = unquote(nombre_buscado)
                
                # BUSCAMOS EN EL MAPA REAL
                real_key = mapa_nube.get(nombre_buscado)
                
                if real_key:
                    try:
                        # Descargar desde la nube
                        file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=real_key)
                        file_content = file_obj['Body'].read()
                        
                        # --- SANITIZACIÓN DE NOMBRE (SOLUCIÓN AL DOCX CORRUPTO) ---
                        # 1. Quitamos espacios y paréntesis que rompen los ZIPs a veces
                        nombre_seguro = re.sub(r'[^\w\.-]', '_', nombre_buscado)
                        
                        # 2. Construimos el nombre final dentro del ZIP
                        # Quedará algo como: evidencia_123_Proyecto_Grado_Oficial.docx
                        nombre_zip = f"evidencia_{id_ev}_{nombre_seguro}"
                        
                        # Guardar en ZIP
                        zip_file.writestr(nombre_zip, file_content)
                        
                    except Exception as e:
                        print(f"⚠️ Error silencioso descargando {id_ev}: {e}")
                        # Ya no agregamos archivos .txt con errores al ZIP, solo lo saltamos.

        zip_buffer.seek(0)
        fecha_str = ahora_ecuador().strftime("%Y%m%d_%H%M")
        
        return StreamingResponse(
            zip_buffer, 
            media_type="application/zip", 
            headers={"Content-Disposition": f"attachment; filename=multimedia_limpio_{fecha_str}.zip"}
        )

    except Exception as e:
        print(f"❌ Error crítico multimedia: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

from urllib.parse import urlparse
import re
import os

@app.post("/optimizar_sistema")
async def optimizar_sistema(tipo: str = "full"):
    """
    V8.3 - Mantenimiento con manejo de errores y VACUUM en conexión independiente.
    """
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            return JSONResponse(
                status_code=500,
                content={"status": "error", "mensaje": "No se pudo conectar a la base de datos"}
            )
        c = conn.cursor(cursor_factory=RealDictCursor)
        mensaje_resultado = []

        if tipo in ["duplicados", "full"]:
            print("🧹 [1/4] Buscando duplicados...")
            c.execute("""
                SELECT hash, ci_estudiante, COUNT(*) as cantidad
                FROM Evidencias
                WHERE hash NOT IN ('PENDIENTE', '', 'RECUPERADO')
                GROUP BY hash, ci_estudiante
                HAVING COUNT(*) > 1
            """)
            grupos = c.fetchall()
            eliminados = 0
            for g in grupos:
                hash_val = g.get('hash') or g.get('Hash')
                cedula = g.get('ci_estudiante') or g.get('CI_Estudiante')
                if not hash_val or not cedula:
                    continue
                c.execute("""
                    SELECT id FROM Evidencias
                    WHERE hash = %s AND ci_estudiante = %s
                    ORDER BY id ASC
                """, (hash_val, cedula))
                copias = c.fetchall()
                for copia in copias[1:]:
                    copia_id = copia.get('id') or copia.get('ID')
                    if copia_id:
                        c.execute("DELETE FROM Evidencias WHERE id = %s", (copia_id,))
                        eliminados += 1
            if eliminados:
                mensaje_resultado.append(f"Se eliminaron {eliminados} duplicados internos.")
            conn.commit()

        if tipo in ["huerfanos", "full"]:
            print("👻 [2/4] Analizando huérfanos...")
            c.execute("SELECT id, url_archivo FROM Evidencias")
            todas = c.fetchall()
            elim_huerfanos = 0
            elim_locales = 0
            for ev in todas:
                ev_id = ev.get('id') or ev.get('ID')
                url = ev.get('url_archivo') or ev.get('Url_Archivo') or ''
                if not ev_id or not url:
                    continue
                if "/local/" in url:
                    c.execute("DELETE FROM Evidencias WHERE id = %s", (ev_id,))
                    elim_locales += 1
                    continue
                existe = True
                if "backblazeb2.com" in url and s3_client:
                    try:
                        if f"/file/{BUCKET_NAME}/" in url:
                            key = url.split(f"/file/{BUCKET_NAME}/")[1]
                            s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
                    except Exception as e:
                        if "404" in str(e) or "Not Found" in str(e):
                            existe = False
                if not existe:
                    c.execute("DELETE FROM Evidencias WHERE id = %s", (ev_id,))
                    elim_huerfanos += 1
            if elim_locales:
                mensaje_resultado.append(f"Se eliminaron {elim_locales} archivos locales corruptos.")
            if elim_huerfanos:
                mensaje_resultado.append(f"Se eliminaron {elim_huerfanos} archivos huérfanos.")
            conn.commit()

                # ========== 3. PENDIENTES ==========
        if tipo in ["full", "pendientes"]:
            print("🧹 [3/4] Eliminando pendientes...")
            # Usamos nombres en minúsculas para coincidir con RealDictCursor
            c.execute("SELECT id, url_archivo, hash FROM Evidencias WHERE ci_estudiante = 'PENDIENTE'")
            pendientes = c.fetchall()
            eliminados = 0
            for row in pendientes:
                ev_id = row.get('id') or row.get('ID')
                url = row.get('url_archivo') or row.get('Url_Archivo') or ''
                file_hash = row.get('hash') or row.get('Hash') or ''
                if not ev_id:
                    continue
                # Verificar si hay otros estudiantes con el mismo hash
                if file_hash:
                    c.execute("SELECT COUNT(*) as total FROM Evidencias WHERE hash = %s AND ci_estudiante != 'PENDIENTE'", (file_hash,))
                    count_res = c.fetchone()
                    total_restantes = count_res.get('total') if count_res else 0
                    if total_restantes == 0 and url and "backblazeb2.com" in url and s3_client:
                        try:
                            if f"/file/{BUCKET_NAME}/" in url:
                                key = url.split(f"/file/{BUCKET_NAME}/")[1]
                                s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
                                print(f"🗑️ Archivo pendiente eliminado de S3: {key}")
                        except Exception as e:
                            print(f"⚠️ Error eliminando archivo S3: {e}")
                c.execute("DELETE FROM Evidencias WHERE id = %s", (ev_id,))
                eliminados += 1
            if eliminados:
                # Eliminar solicitudes que referencien a estas evidencias (ahora huérfanas)
                c.execute("DELETE FROM Solicitudes WHERE Id_Evidencia IN (SELECT id FROM Evidencias WHERE ci_estudiante = 'PENDIENTE')")
                mensaje_resultado.append(f"Se eliminaron {eliminados} evidencias pendientes y sus archivos S3 (si no estaban en uso).")
            else:
                mensaje_resultado.append("No se encontraron evidencias pendientes.")
            conn.commit()

        # ========== 4. VACUUM (Conexión independiente) ==========
        if tipo in ["cache", "full"]:
            print("🧹 [4/4] Compactando base de datos...")
            try:
                vac_conn = get_db_connection()
                if vac_conn:
                    vac_conn.autocommit = True
                    with vac_conn.cursor() as c_vac:
                        c_vac.execute("VACUUM")
                        c_vac.execute("ANALYZE")
                    vac_conn.close()
                    mensaje_resultado.append("Base de datos compactada correctamente.")
                else:
                    mensaje_resultado.append("Advertencia: No se pudo conectar para VACUUM.")
            except Exception as e_vac:
                mensaje_resultado.append(f"Advertencia: VACUUM falló ({str(e_vac)}).")

        texto_final = " ".join(mensaje_resultado) if mensaje_resultado else "Sistema optimizado. Todo limpio."
        return JSONResponse({"status": "ok", "mensaje": texto_final})

    except Exception as e:
        import traceback
        error_detalle = traceback.format_exc()
        print(f"❌ Error en mantenimiento: {error_detalle}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "mensaje": str(e),
                "detalle": error_detalle
            }
        )
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass
    
# =========================================================================
# 10. ENDPOINTS DE ESTADÍSTICAS Y REPORTES
# =========================================================================

@app.get("/estadisticas_almacenamiento")
def estadisticas_almacenamiento():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Contar Usuarios (Ignorando al admin tipo 0)
        c.execute("SELECT COUNT(*) FROM Usuarios WHERE Tipo != 0")
        usuarios_activos = c.fetchone()['count']
        
        # 2. Contar Evidencias Totales
        c.execute("SELECT COUNT(*) FROM Evidencias")
        total_evidencias = c.fetchone()['count']
        
        # 3. 🔥 SOLICITUDES PENDIENTES (CORREGIDO)
        # Usamos UPPER() para que 'Pendiente', 'pendiente' y 'PENDIENTE' cuenten igual.
        c.execute("SELECT COUNT(*) FROM Solicitudes WHERE UPPER(Estado) = 'PENDIENTE'")
        solicitudes_pendientes = c.fetchone()['count']

        # 4. 🔥 ALMACENAMIENTO (CORREGIDO - USANDO BASE DE DATOS)
        # Sumamos la columna Tamanio_KB. Si es null, devuelve 0.
        c.execute("SELECT COALESCE(SUM(Tamanio_KB), 0) as total_kb FROM Evidencias")
        total_kb = c.fetchone()['total_kb']
        
        # Convertimos KB a GB (1 GB = 1024*1024 KB)
        gb_usados = float(total_kb) / (1024 * 1024)
        
        # 5. Costos Estimados
        costo_storage = gb_usados * 0.023
        costo_ia = total_evidencias * 0.001
        
        return JSONResponse({
            "usuarios_activos": usuarios_activos,
            "total_evidencias": total_evidencias,
            "solicitudes_pendientes": solicitudes_pendientes, # Dato exacto
            "almacenamiento_gb": gb_usados,                   # Dato exacto
            "costo_estimado_usd": {
                "storage": costo_storage,
                "rekognition": costo_ia,
                "total": costo_storage + costo_ia
            }
        })
        
    except Exception as e:
        print(f"Error estadisticas: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    finally:
        if conn: conn.close()

@app.get("/datos_graficos_dashboard")
async def datos_graficos_dashboard():
    conn = None # <--- 1. Inicializar
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # 1. Evolución de registros por mes (TO_CHAR en lugar de strftime)
        c.execute("""
            SELECT TO_CHAR(Fecha_Registro, 'YYYY-MM') as mes,
                   COUNT(*) as cantidad
            FROM Usuarios 
            WHERE Fecha_Registro IS NOT NULL
            GROUP BY mes
            ORDER BY mes DESC
            LIMIT 12
        """)
        evolucion_usuarios = [dict(row) for row in c.fetchall()]
        
        # 2. Distribución de tipos de archivo
        c.execute("""
            SELECT Tipo_Archivo, COUNT(*) as cantidad
            FROM Evidencias
            GROUP BY Tipo_Archivo
        """)
        distribucion_archivos = [dict(row) for row in c.fetchall()]
        
        # 3. Solicitudes por estado
        c.execute("""
            SELECT Estado, COUNT(*) as cantidad
            FROM Solicitudes
            GROUP BY Estado
        """)
        solicitudes_estado = [dict(row) for row in c.fetchall()]
        
        # 4. Top 5 estudiantes
        c.execute("""
            SELECT u.Nombre, u.Apellido, u.CI, COUNT(e.id) as total
            FROM Usuarios u
            LEFT JOIN Evidencias e ON u.CI = e.CI_Estudiante
            WHERE u.Tipo = 1
            GROUP BY u.CI
            ORDER BY total DESC
            LIMIT 5
        """)
        top_estudiantes = [dict(row) for row in c.fetchall()]
        
        # 5. Actividad por hora (TO_CHAR y sintaxis de intervalo Postgres)
        c.execute("""
            SELECT TO_CHAR(Fecha, 'HH24') as hora,
                   COUNT(*) as actividades
            FROM Auditoria
            WHERE Fecha >= NOW() - INTERVAL '7 days'
            GROUP BY hora
            ORDER BY hora
        """)
        actividad_horaria = [dict(row) for row in c.fetchall()]
        
        return JSONResponse(content={
            "evolucion_usuarios": evolucion_usuarios,
            "distribucion_archivos": distribucion_archivos,
            "solicitudes_estado": solicitudes_estado,
            "top_estudiantes": top_estudiantes,
            "actividad_horaria": actividad_horaria,
            "fecha_consulta": ahora_ecuador().isoformat()
        })
        
    except Exception as e:
        return JSONResponse(content={"error": str(e)})
    finally: # <--- 3. CERRAR SIEMPRE
        if conn: conn.close()

# =========================================================================
# 11. ENDPOINTS DE SOLICITUDES Y GESTIÓN
# =========================================================================
@app.get("/obtener_solicitudes")
def obtener_solicitudes(limit: int = 100):
    try:
        conn = get_db_connection()
        # 1. RESTAURADO: RealDictCursor es OBLIGATORIO para que el Frontend entienda los datos
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 2. RESTAURADO: El LEFT JOIN es vital para ver las fotos/videos en el Admin
        query = """
            SELECT 
                s.*, 
                e.Url_Archivo, 
                e.Tipo_Archivo
            FROM Solicitudes s
            LEFT JOIN Evidencias e ON s.Id_Evidencia = e.id
            ORDER BY s.Fecha DESC 
            LIMIT %s
        """
        c.execute(query, (limit,))
        solicitudes = c.fetchall()
        conn.close()
        
        # ✅ TU CORRECCIÓN MÁGICA (Funciona perfecto, la dejamos)
        sol_serializables = json.loads(json.dumps(solicitudes, default=str))
        
        return JSONResponse(sol_serializables)
        
    except Exception as e:
        print(f"❌ Error obteniendo solicitudes: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    
# =========================================================================
# 1. ENDPOINTS DE SOLICITUDES (LADO ESTUDIANTE) - ¡ESTO ES LO QUE TE FALTA!
# =========================================================================

@app.post("/solicitar_recuperacion")
async def solicitar_recuperacion(
    background_tasks: BackgroundTasks,
    cedula: str = Form(...),
    email: Optional[str] = Form(None),
    mensaje: Optional[str] = Form(None),
    tipo: str = Form("RECUPERACION_CONTRASENA")
):
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Buscar al usuario
        c.execute("SELECT Nombre, Apellido, Email FROM Usuarios WHERE CI=%s", (cedula.strip(),))
        user = c.fetchone()
        
        if not user:
            return JSONResponse({"status": "error", "mensaje": "La cédula no está registrada."})
            
        email_final = email if email else user.get('email') or user.get('Email') or "Sin correo"
        nombre_completo = f"{user.get('nombre') or user.get('Nombre')} {user.get('apellido') or user.get('Apellido')}"
        
        detalle = f"{tipo.replace('_', ' ')}. "
        if mensaje: detalle += f"Mensaje: {mensaje}"
        
        # 2. Guardar en Base de Datos (sin enviar correos)
        c.execute("""
            INSERT INTO Solicitudes (Tipo, CI_Solicitante, Email, Detalle, Estado, Fecha)
            VALUES (%s, %s, %s, %s, 'PENDIENTE', %s)
        """, (tipo, cedula.strip(), email_final, detalle, ahora_ecuador()))
        
        conn.commit()

        return JSONResponse({"status": "ok", "mensaje": "Solicitud enviada correctamente."})
    except Exception as e:
        return JSONResponse({"status": "error", "mensaje": str(e)})
    finally:
        if conn: conn.close()

@app.post("/solicitar_subida")
async def solicitar_subida(
    cedula: str = Form(...),
    archivo: UploadFile = File(...)
):
    """
    Sube el archivo pero lo deja OCULTO (Estado 0) y crea una solicitud
    para que el admin lo revise.
    """
    conn = None
    temp_dir = None
    try:
        # 1. Guardar archivo físico
        temp_dir = tempfile.mkdtemp()
        path = os.path.join(temp_dir, archivo.filename)
        with open(path, "wb") as f:
            shutil.copyfileobj(archivo.file, f)
            
        file_hash = calcular_hash(path)
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)

        # 2. Subir a la Nube (S3/Backblaze)
        path_procesado = garantizar_limite_storage(path)
        tamanio_kb = os.path.getsize(path_procesado) / 1024
        
        # Nombre único
        nombre_nube = f"evidencias/pendiente_{int(ahora_ecuador().timestamp())}_{archivo.filename}"
        url_final = f"/local/{archivo.filename}" # Fallback local
        
        if s3_client:
            s3_client.upload_file(path_procesado, BUCKET_NAME, nombre_nube, ExtraArgs={'ACL': 'public-read'})
            url_final = f"https://{BUCKET_NAME}.s3.us-east-005.backblazeb2.com/{nombre_nube}"

        # 3. Insertar Evidencia en BD con ESTADO = 0 (Oculta/Pendiente)
        # Tipo: Determinamos si es imagen o video
        ext = os.path.splitext(archivo.filename)[1].lower()
        tipo_archivo = "video" if ext in ['.mp4', '.mov', '.avi'] else "imagen"

        c.execute("""
            INSERT INTO Evidencias (CI_Estudiante, Url_Archivo, Hash, Estado, Tipo_Archivo, Tamanio_KB, Asignado_Automaticamente)
            VALUES (%s, %s, %s, 0, %s, %s, 0) RETURNING id
        """, (cedula, url_final, file_hash, tipo_archivo, tamanio_kb))
        
        id_evidencia = c.fetchone()['id']

        # 4. Crear la Solicitud para el Admin
        # Obtenemos nombre para el correo
        c.execute("SELECT Nombre, Apellido, Email FROM Usuarios WHERE CI=%s", (cedula,))
        user = c.fetchone()
        email = user['email'] if user else 'Sin correo'
        
        c.execute("""
            INSERT INTO Solicitudes (Tipo, CI_Solicitante, Email, Detalle, Id_Evidencia, Estado, Fecha)
            VALUES ('SUBIR_EVIDENCIA', %s, %s, 'Estudiante solicita subir este archivo.', %s, 'PENDIENTE', %s)
        """, (cedula, email, id_evidencia, ahora_ecuador()))

        conn.commit()
        return JSONResponse({"status": "ok", "mensaje": "Archivo enviado a revisión del administrador."})

    except Exception as e:
        if conn: conn.rollback()
        return JSONResponse({"status": "error", "mensaje": str(e)})
    finally:
        if conn: conn.close()
        if temp_dir and os.path.exists(temp_dir): shutil.rmtree(temp_dir)

@app.post("/reportar_evidencia")
async def reportar_evidencia(
    background_tasks: BackgroundTasks,
    id_evidencia: int = Form(...),
    motivo: str = Form(...),
    cedula: str = Form(...) # ✅ CORREGIDO: Ahora coincide con tu HTML
):
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Obtener datos del usuario (para registro interno)
        c.execute("SELECT * FROM Usuarios WHERE CI = %s", (cedula.strip(),))
        usuario = c.fetchone()
        email_usuario = usuario.get('email') if usuario else 'Sin correo'
        
        # 2. Crear la Solicitud en la Base de Datos
        # Esto hace que aparezca en tu Panel de Admin
        detalle_reporte = f"Reporte de evidencia ID {id_evidencia}. Motivo: {motivo}"
        
        c.execute("""
            INSERT INTO Solicitudes (Tipo, CI_Solicitante, Email, Detalle, Id_Evidencia, Estado, Fecha)
            VALUES ('REPORTE_EVIDENCIA', %s, %s, %s, %s, 'PENDIENTE', %s)
        """, (cedula, email_usuario, detalle_reporte, id_evidencia, ahora_ecuador()))
        
        conn.commit()
        
        # ❌ SIN CORREO: Hemos quitado la línea que enviaba emails para evitar SPAM.
        
        return JSONResponse({"status": "ok", "mensaje": "Reporte enviado al administrador."})

    except Exception as e:
        if conn: conn.rollback()
        print(f"Error en reporte: {e}")
        return JSONResponse({"status": "error", "mensaje": str(e)})
    finally:
        if conn: conn.close()

@app.get("/obtener_solicitudes_por_cedula")
async def obtener_solicitudes_por_cedula(cedula: str):
    """
    Obtiene historial y gestiona el AUTOBORRADO DE 1 HORA.
    Lógica:
    1. Si ya pasaron 60 min desde 'Fecha_Visto', se borra automáticamente.
    2. Si es nueva (Fecha_Visto es NULL) y ya está RESUELTA, le ponemos la hora actual (empieza el temporizador).
    3. Devuelve las que quedan.
    """
    conn = None
    try:
        conn = get_db_connection()
        # Usamos cursor para evitar errores en Postgres
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        cedula = cedula.strip()

        # --- A) LIMPIEZA AUTOMÁTICA (El temporizador de 1 hora) ---
        # Borra solicitudes que NO son pendientes y que fueron vistas hace más de 1 hora
        c.execute("""
            DELETE FROM Solicitudes 
            WHERE CI_Solicitante = %s 
            AND Estado != 'PENDIENTE' 
            AND Fecha_Visto IS NOT NULL 
            AND Fecha_Visto < (NOW() AT TIME ZONE 'America/Guayaquil' - INTERVAL '1 hour')
        """, (cedula,))
        
        filas_borradas = c.rowcount
        if filas_borradas > 0:
            print(f"🧹 Auto-limpieza: Se borraron {filas_borradas} notificaciones antiguas de {cedula}")

        # --- B) MARCAR COMO VISTAS (Iniciar el temporizador) ---
        # Si el estudiante está viendo esto ahora, marcamos la hora actual
        # Solo para las que ya están resueltas (APROBADA/RECHAZADA) y no tenían fecha
        c.execute("""
            UPDATE Solicitudes 
            SET Fecha_Visto = (NOW() AT TIME ZONE 'America/Guayaquil')
            WHERE CI_Solicitante = %s 
            AND Estado != 'PENDIENTE' 
            AND Fecha_Visto IS NULL
        """, (cedula,))
        
        conn.commit() # Guardamos los cambios de limpieza y marcado

        # --- C) DEVOLVER LAS QUE QUEDAN ---
        c.execute("""
            SELECT * FROM Solicitudes 
            WHERE CI_Solicitante = %s 
            ORDER BY Fecha DESC
        """, (cedula,))
        
        rows = c.fetchall()
        # Convertimos fechas a formato JSON seguro
        return JSONResponse(jsonable_encoder(rows))

    except Exception as e:
        print(f"❌ Error historial estudiante: {e}")
        return JSONResponse([])
    finally:
        if conn: conn.close()

# =========================================================================
# AQUI DEBERÍA SEGUIR TU FUNCIÓN @app.post("/gestionar_solicitud") ...
# =========================================================================

@app.post("/gestionar_solicitud")
async def gestionar_solicitud(
    background_tasks: BackgroundTasks,
    id_solicitud: int = Form(...),
    accion: str = Form(...),  # 'APROBADA' o 'RECHAZADA'
    mensaje: str = Form(...),
    id_admin: str = Form("Administrador")
):
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Obtener datos de la solicitud
        c.execute("SELECT * FROM Solicitudes WHERE ID = %s", (id_solicitud,))
        sol = c.fetchone()

        if not sol:
            return JSONResponse({"status": "error", "mensaje": "Solicitud no encontrada"})

        # Recuperar datos clave
        tipo = sol.get('tipo') or sol.get('Tipo')
        id_evidencia = sol.get('id_evidencia') or sol.get('Id_Evidencia')
        email_usuario = sol.get('email') or sol.get('Email')

        # ---------------------------------------------------------
        # 2. LÓGICA DE GESTIÓN (BORRADO INTELIGENTE)
        # ---------------------------------------------------------

        # --- CASO A: REPORTE "NO SOY YO" ---
        if tipo == 'REPORTE_EVIDENCIA':
            if accion == 'APROBADA':
                if id_evidencia:
                    c.execute("SELECT Url_Archivo, Hash FROM Evidencias WHERE id = %s", (id_evidencia,))
                    ev_data = c.fetchone()
                    if ev_data:
                        # Desvincular
                        c.execute("DELETE FROM Evidencias WHERE id = %s", (id_evidencia,))
                        print(f"✅ Evidencia {id_evidencia} eliminada del perfil del usuario.")
                        
                        file_hash = ev_data.get('Hash') or ev_data.get('hash')
                        url = ev_data.get('Url_Archivo') or ev_data.get('url_archivo')
                        
                        c.execute("SELECT COUNT(*) as total FROM Evidencias WHERE Hash = %s", (file_hash,))
                        count_res = c.fetchone()
                        total_restantes = count_res['total'] if count_res else 0
                        
                        if total_restantes == 0 and url and s3_client and BUCKET_NAME and "backblazeb2.com" in url:
                            try:
                                if f"/file/{BUCKET_NAME}/" in url:
                                    file_key = url.split(f"/file/{BUCKET_NAME}/")[1]
                                    s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_key)
                                    print(f"🗑️ Archivo físico eliminado de la nube: {file_key}")
                            except Exception as e:
                                print(f"⚠️ Error borrando de S3: {e}")
            # Si es RECHAZADA, no hacemos nada

        # --- CASO B: SUBIR EVIDENCIA ---
        elif tipo == 'SUBIR_EVIDENCIA':
            if accion == 'APROBADA':
                if id_evidencia:
                    c.execute("UPDATE Evidencias SET Estado = 1 WHERE id = %s", (id_evidencia,))
            else:  # RECHAZADA
                if id_evidencia:
                    c.execute("SELECT Url_Archivo FROM Evidencias WHERE id = %s", (id_evidencia,))
                    ev_data = c.fetchone()
                    if ev_data:
                        url = ev_data.get('Url_Archivo')
                        if url and s3_client and BUCKET_NAME and f"/file/{BUCKET_NAME}/" in url:
                            try:
                                key = url.split(f"/file/{BUCKET_NAME}/")[1]
                                s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
                            except:
                                pass
                    c.execute("DELETE FROM Evidencias WHERE id = %s", (id_evidencia,))

        # --- CASO C: RECUPERACIÓN DE CONTRASEÑA ---
        elif tipo == 'RECUPERACION_CONTRASENA':
            if accion == 'APROBADA':
                asunto = "🔐 Recuperación de Acceso - U.E. Despertar"
                cuerpo = f"""
                <h3>Hola, hemos procesado tu solicitud.</h3>
                <p>El administrador ha revisado tu caso.</p>
                <p><strong>Tu contraseña/respuesta es:</strong></p>
                <h2 style="color:#6A0DAD;">{mensaje}</h2>
                <hr>
                <p>Intenta ingresar nuevamente.</p>
                """
                if email_usuario and '@' in email_usuario:
                    background_tasks.add_task(enviar_correo_real, email_usuario, asunto, cuerpo)

        # ---------------------------------------------------------
        # 3. ACTUALIZAR HISTORIAL DE LA SOLICITUD (SIEMPRE)
        # ---------------------------------------------------------
        c.execute("""
            UPDATE Solicitudes 
            SET Estado = %s, Respuesta = %s, Resuelto_Por = %s, Fecha_Resolucion = %s
            WHERE ID = %s
        """, (accion, mensaje, id_admin, ahora_ecuador(), id_solicitud))

        tipo_sol = sol.get('tipo') or sol.get('Tipo')
        registrar_auditoria(
            "GESTION_SOLICITUD",
            f"Admin {accion} la solicitud de {tipo_sol} con respuesta: '{mensaje}'",
            id_admin
        )

        conn.commit()
        return JSONResponse({"status": "ok", "mensaje": "Acción ejecutada correctamente."})

    except Exception as e:
        if conn:
            conn.rollback()
        print(f"Error gestionando solicitud: {e}")
        return JSONResponse({"status": "error", "mensaje": str(e)})
    finally:
        if conn:
            conn.close()
        
# =========================================================================
# 12. ENDPOINTS DE LOGS Y AUDITORÍA
# =========================================================================

@app.get("/obtener_logs")
def obtener_logs():
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # ✅ AUMENTAMOS EL LÍMITE A 5000 (Suficiente para meses de historial)
        # Si pones sin límite (quitas el LIMIT), podría ponerse lento si tienes 1 millón de registros.
        # 5000 es un buen equilibrio.
        c.execute("SELECT * FROM Auditoria ORDER BY Fecha DESC LIMIT 5000")
        
        logs = c.fetchall()
        conn.close()
        
        # Convertir a JSON seguro
        logs_serializables = json.loads(json.dumps(logs, default=str))
        
        return JSONResponse(logs_serializables)
    except Exception as e:
        print(f"Error logs: {e}")
        return JSONResponse([])
    
# =========================================================================
# 13. ENDPOINTS EXISTENTES MANTENIDOS
# =========================================================================

@app.get("/listar_usuarios")
def listar_usuarios():
    """Versión OPTIMIZADA: Lista usuarios rápidamente"""
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # Traemos solo los campos necesarios, ordenados alfabéticamente
        c.execute("""
        SELECT ID, Nombre, Apellido, CI, Tipo, Foto, Activo, Email, Telefono 
        FROM Usuarios 
        ORDER BY Apellido ASC, Nombre ASC
    """)
        usuarios = c.fetchall()
        conn.close()
        
        # Serialización segura de fechas y datos
        return JSONResponse(json.loads(json.dumps(usuarios, default=str)))
        
    except Exception as e:
        print(f"❌ Error listando usuarios: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    
@app.get("/resumen_estudiantes_con_evidencias")
def resumen_estudiantes():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Consulta segura
        query = """
            SELECT 
                u.Nombre, u.Apellido, u.CI, u.Foto,
                COUNT(e.id) as total_evidencias,
                COALESCE(SUM(e.Tamanio_KB), 0) as total_kb
            FROM Usuarios u
            LEFT JOIN Evidencias e ON u.CI = e.CI_Estudiante
            WHERE u.Tipo = 1
            GROUP BY u.CI, u.Nombre, u.Apellido, u.Foto
            ORDER BY u.Apellido ASC
        """
        c.execute(query)
        data = c.fetchall()
        conn.close()
        
        # ✅ CORRECCIÓN: Convertir fechas y datos raros a texto
        import json
        data_serializable = json.loads(json.dumps(data, default=str))
        
        return JSONResponse(data_serializable)
    except Exception as e:
        print(f"❌ Error resumen: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    
@app.get("/todas_evidencias")
def todas_evidencias(cedula: str):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Buscamos por la cédula del estudiante
        c.execute("SELECT * FROM Evidencias WHERE CI_Estudiante = %s ORDER BY id DESC", (cedula,))
        evs = c.fetchall()
        conn.close()
        
        # ✅ CORRECCIÓN: Convertir fechas a texto para que no falle
        import json
        evs_serializables = json.loads(json.dumps(evs, default=str))
        
        return JSONResponse(evs_serializables)
    except Exception as e:
        print(f"❌ Error evidencias estudiante: {e}")
        return JSONResponse([])

@app.delete("/eliminar_evidencia/{id}")
async def eliminar_evidencia(id: int, admin_cedula: str = Form(...)): # 1. Pedir quién es
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 2. Verificar que sea Admin
        c.execute("SELECT Tipo FROM Usuarios WHERE CI = %s", (admin_cedula,))
        admin = c.fetchone()
        if not admin or admin['Tipo'] != 0:
             conn.close()
             return JSONResponse({"error": "No autorizado"}, status_code=403)
        
        # 1. Buscar la evidencia
        c.execute("SELECT * FROM Evidencias WHERE id = %s", (id,))
        evidencia = c.fetchone()
        
        if not evidencia:
            conn.close()
            raise HTTPException(status_code=404, detail="Evidencia no encontrada")
            
        # 🛡️ CORRECCIÓN: Buscamos la URL con seguridad (mayúsculas o minúsculas)
        url = evidencia.get('Url_Archivo') or evidencia.get('url_archivo')
        
        # 2. Borrar de la Nube (Si tiene URL válida)
        if url and s3_client and BUCKET_NAME and "backblazeb2.com" in url:
            try:
                # Extraer la clave del archivo
                partes = url.split(f"/file/{BUCKET_NAME}/")
                if len(partes) > 1:
                    file_key = partes[1]
                    print(f"🗑️ Eliminando de B2: {file_key}")
                    s3_client.delete_object(Bucket=BUCKET_NAME, Key=file_key)
            except Exception as e_b2:
                print(f"⚠️ Alerta: Se borró de BD pero falló en B2: {e_b2}")

        # 3. Borrar de la Base de Datos
        c.execute("DELETE FROM Evidencias WHERE id = %s", (id,))
        conn.commit()
        conn.close()
        
        return JSONResponse({"mensaje": "Evidencia eliminada correctamente"})
        
    except Exception as e:
        # Imprimimos el error exacto en los logs de Railway
        print(f"❌ Error CRÍTICO eliminando evidencia {id}: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)
    
@app.get("/diagnostico_usuario/{cedula}")
async def diagnostico_usuario(cedula: str):
    """Diagnóstico completo de un usuario (Versión PostgreSQL)"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # CORRECCIÓN: Usamos information_schema en lugar de PRAGMA
        c.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'usuarios'
        """)
        columnas = c.fetchall()
        
        # Buscar usuario
        c.execute("SELECT * FROM Usuarios WHERE CI = %s", (cedula,))
        usuario = c.fetchone()
        
        # Evidencias del usuario
        c.execute("""
            SELECT COUNT(*) as total, 
                   SUM(Tamanio_KB) as total_kb,
                   Tipo_Archivo,
                   COUNT(*) as cantidad
            FROM Evidencias
            WHERE CI_Estudiante = %s
            GROUP BY Tipo_Archivo
        """, (cedula,))
        estadisticas_evidencias = [dict(r) for r in c.fetchall()]
        
        # Solicitudes del usuario
        c.execute("""
            SELECT Estado, COUNT(*) as cantidad
            FROM Solicitudes
            WHERE CI_Solicitante = %s
            GROUP BY Estado
        """, (cedula,))
        estadisticas_solicitudes = [dict(r) for r in c.fetchall()]
        
        conn.close()
        
        return JSONResponse(content={
            "cedula_buscada": cedula,
            "usuario_encontrado": bool(usuario),
            "usuario": dict(usuario) if usuario else None,
            "estructura_tabla": [dict(r) for r in columnas],
            "estadisticas_evidencias": estadisticas_evidencias,
            "estadisticas_solicitudes": estadisticas_solicitudes,
            "fecha_diagnostico": ahora_ecuador().isoformat(),
            "zona_horaria": "America/Guayaquil (UTC-5)"
        })
        
    except Exception as e:
        return JSONResponse(content={"error": str(e)})

@app.get("/reset-db")
async def reset_database():
    """Reinicia la base de datos (SOLO DESARROLLO)"""
    try:
        init_db_completa()
        return JSONResponse(content={
            "status": "ok",
            "mensaje": "Base de datos reinicializada",
            "fecha": ahora_ecuador().isoformat()
        })
    except Exception as e:
        return JSONResponse(content={"error": str(e)})
    


# =========================================================================
# 14. ENDPOINTS CORS Y UTILIDADES
# =========================================================================

@app.get("/cors-debug")
async def cors_debug():
    """Endpoint para debug de CORS"""
    return JSONResponse(content={
        "message": "CORS Debug Endpoint",
        "allow_origin": "*",
        "allow_methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH",
        "allow_headers": "*",
        "allow_credentials": "true",
        "timestamp": ahora_ecuador().isoformat(),
        "zona_horaria": "America/Guayaquil (UTC-5)"
    })

# =========================================================================
# 15. INICIO DE LA APLICACIÓN
# =========================================================================

class PasswordRequest(BaseModel):
    cedula: str
    nueva_contrasena: str

@app.post("/cambiar_contrasena")
async def cambiar_contrasena(datos: PasswordRequest):
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        c.execute("SELECT Nombre, Apellido FROM Usuarios WHERE CI = %s", (datos.cedula,))
        u = c.fetchone()
        nombre_usuario = f"{u['nombre']} {u['apellido']}" if u else datos.cedula

        # --- CORRECCIÓN DE SEGURIDAD ---
        # 1. Encriptamos la nueva contraseña antes de actualizar
        hashed_password = get_password_hash(datos.nueva_contrasena)

        # 2. Guardamos el HASH, no el texto plano
        c.execute("UPDATE Usuarios SET Password = %s WHERE CI = %s", (hashed_password, datos.cedula))
        conn.commit()
        
        registrar_auditoria(
            "CAMBIO_PASSWORD", 
            f"El Admin cambió la contraseña del usuario: {nombre_usuario} (CI: {datos.cedula})", 
            "Administrador"
        )
        
        return JSONResponse({"mensaje": "Contraseña actualizada correctamente"})
    except Exception as e:
        return JSONResponse({"error": str(e)})
    finally:
        if conn: conn.close()
    
@app.post("/descargar_evidencias_zip")
async def descargar_evidencias_zip(ids: str = Form(...)):
    try:
        from urllib.parse import unquote # Importante para decodificar URLs
        
        lista_ids = ids.split(',')
        if not lista_ids:
            return JSONResponse({"error": "No hay IDs"}, status_code=400)
            
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # Consultar archivos
        # Usamos una consulta segura para obtener solo los IDs solicitados
        placeholders = ','.join(['%s'] * len(lista_ids))
        c.execute(f"SELECT id, Url_Archivo FROM Evidencias WHERE id IN ({placeholders})", tuple(lista_ids))
        resultados = c.fetchall()
        conn.close()
        
        if not resultados:
            return JSONResponse({"error": "No se encontraron archivos en la base de datos"}, status_code=404)

        # Crear ZIP en memoria
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for item in resultados:
                # 1. Obtener y limpiar datos
                raw_url = item.get('Url_Archivo') or item.get('url_archivo') or ""
                id_ev = item.get('id')
                
                if not raw_url: continue

                # Decodificar URL (ej: cambia %20 por espacios)
                url = unquote(raw_url)
                
                # Nombre limpio para el archivo dentro del ZIP
                nombre_base = os.path.basename(url)
                # Quitamos caracteres que Windows odia
                nombre_base = "".join([c for c in nombre_base if c.isalnum() or c in "._- "])
                nombre_zip = f"evidencia_{id_ev}_{nombre_base}"
                
                archivo_agregado = False

                # CASO 1: Archivo en Nube (Backblaze/S3)
                if "backblazeb2.com" in url or "s3" in url:
                    if s3_client and BUCKET_NAME:
                        try:
                            # Lógica inteligente para encontrar la 'Key' (ruta interna)
                            file_key = None
                            
                            if f"/file/{BUCKET_NAME}/" in url:
                                file_key = url.split(f"/file/{BUCKET_NAME}/")[1]
                            elif ".com/" in url:
                                # Intento secundario de encontrar la ruta después del dominio
                                file_key = url.split(".com/")[1]
                            
                            if file_key:
                                # Descargar de S3 a memoria
                                file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
                                file_content = file_obj['Body'].read()
                                zip_file.writestr(nombre_zip, file_content)
                                archivo_agregado = True
                            else:
                                raise Exception("No se pudo extraer la clave del archivo de la URL")
                                
                        except Exception as e:
                            print(f"⚠️ Error bajando ID {id_ev}: {e}")
                            # EN LUGAR DE ROMPERSE (ERROR 500), CREAMOS UN TXT DE ERROR
                            zip_file.writestr(f"ERROR_ID_{id_ev}.txt", f"No se pudo descargar este archivo.\nURL: {url}\nError: {str(e)}")
                            archivo_agregado = True # Marcamos como procesado para no duplicar lógica
                
                # CASO 2: Archivo Local (Fallback)
                if not archivo_agregado and "/local/" in url:
                    # Intento de ruta local (si aplica)
                    try:
                         # Limpiamos la ruta para buscar en el servidor
                         local_path = url.replace("/local/", "").lstrip("/")
                         full_local_path = os.path.join(BASE_DIR, local_path)
                         
                         if os.path.exists(full_local_path):
                             zip_file.write(full_local_path, nombre_zip)
                         else:
                             zip_file.writestr(f"MISSING_ID_{id_ev}.txt", "El archivo local no existe en el servidor.")
                    except Exception as e:
                        zip_file.writestr(f"ERROR_LOCAL_{id_ev}.txt", str(e))

        # Finalizar el ZIP
        zip_buffer.seek(0)
        
        # Nombre del ZIP con fecha
        fecha_str = ahora_ecuador().strftime("%Y%m%d_%H%M")
        nombre_descarga = f"seleccion_evidencias_{fecha_str}.zip"
        
        return StreamingResponse(
            zip_buffer, 
            media_type="application/zip",
            headers={
                "Content-Disposition": f"attachment; filename={nombre_descarga}",
                "Access-Control-Expose-Headers": "Content-Disposition"
            }
        )

    except Exception as e:
        print(f"❌ Error CRÍTICO generando ZIP: {e}")
        return JSONResponse({"error": f"Error interno del servidor: {str(e)}"}, status_code=500)

def limpieza_duplicados_startup():
    """
    V9.0 - MANTENIMIENTO INTELIGENTE (MULTI-USUARIO)
    - Fases 0 y 2 corregidas: Solo borran duplicados si pertenecen AL MISMO ESTUDIANTE.
      (Permite que varios alumnos compartan la misma evidencia/URL sin que se borre).
    - Fase 4: Modo Seguro (No borra, solo avisa).
    """
    print("🧹 INICIANDO PROTOCOLO DE LIMPIEZA Y MANTENIMIENTO (MODO COMPARTIDO)...")
    
    conn = None
    try:
        conn = get_db_connection()
        # Usamos RealDictCursor para acceder por nombres de columna (soportando mayús/minús)
        c = conn.cursor(cursor_factory=RealDictCursor) 
        
        # =========================================================
        # FASE 0: LIMPIEZA POR URL (SÓLO SI ES EL MISMO DUEÑO)
        # =========================================================
        print("🔍 FASE 0: Buscando URLs duplicadas en el mismo perfil...")
        # CAMBIO CLAVE: Agrupamos por URL *Y* CI_Estudiante
        c.execute("""
            SELECT Url_Archivo, CI_Estudiante, COUNT(*) as cantidad 
            FROM Evidencias 
            WHERE Url_Archivo != ''
            GROUP BY Url_Archivo, CI_Estudiante 
            HAVING COUNT(*) > 1
        """)
        duplicados_url = c.fetchall()
        
        eliminados_0 = 0
        for row in duplicados_url:
            url = row.get('Url_Archivo') or row.get('url_archivo')
            ci = row.get('CI_Estudiante') or row.get('ci_estudiante')
            
            # Borramos las copias extra DE ESE ESTUDIANTE
            c.execute("""
                SELECT id FROM Evidencias 
                WHERE Url_Archivo = %s AND CI_Estudiante = %s 
                ORDER BY id ASC
            """, (url, ci))
            copias = c.fetchall()
            
            # Dejamos el primero (original), borrar el resto
            for copia in copias[1:]: 
                c.execute("DELETE FROM Evidencias WHERE id = %s", (copia['id'],))
                eliminados_0 += 1
        
        if eliminados_0 > 0: print(f"   ✨ Fase 0: {eliminados_0} registros repetidos corregidos.")

        # =========================================================
        # FASE 1: REPARAR ARCHIVOS ANTIGUOS (Generar Hash faltante)
        # =========================================================
        print("⏳ FASE 1: Verificando huellas digitales (Hashes)...")
        c.execute("SELECT id, Url_Archivo FROM Evidencias WHERE Hash = 'PENDIENTE' OR Hash IS NULL")
        pendientes = c.fetchall()
        
        count_hashed = 0
        for row in pendientes:
            try:
                url = row.get('Url_Archivo') or row.get('url_archivo')
                id_ev = row.get('id')
                temp_path = None
                file_hash = None
                
                # Descargar temporalmente para calcular hash
                if url and "http" in url and s3_client:
                    try:
                        parsed = urlparse(url)
                        key = parsed.path.lstrip('/')
                        with tempfile.NamedTemporaryFile(delete=False) as tmp:
                            s3_client.download_fileobj(BUCKET_NAME, key, tmp)
                            temp_path = tmp.name
                        file_hash = calcular_hash(temp_path)
                    except: pass 
                elif url and "/local/" in url:
                    ruta_local = url.replace("/local/", "./")
                    if not os.path.exists(ruta_local):
                         ruta_local = os.path.join(BASE_DIR, url.replace("/local/", "").lstrip("/"))
                    if os.path.exists(ruta_local):
                        file_hash = calcular_hash(ruta_local)

                if file_hash:
                    c.execute("UPDATE Evidencias SET Hash = %s WHERE id = %s", (file_hash, id_ev))
                    count_hashed += 1
                
                if temp_path and os.path.exists(temp_path): os.remove(temp_path)
            except: pass
            
        conn.commit()
        if count_hashed > 0: print(f"   ✨ Fase 1: {count_hashed} archivos reparados.")

        # =========================================================
        # FASE 2: ELIMINAR POR HASH (SÓLO SI ES EL MISMO DUEÑO)
        # =========================================================
        print("🔍 FASE 2: Buscando contenido duplicado en el mismo perfil...")
        # CAMBIO CLAVE: Agrupamos por Hash *Y* CI_Estudiante
        c.execute("""
            SELECT Hash, CI_Estudiante, COUNT(*) as cantidad 
            FROM Evidencias 
            WHERE Hash NOT IN ('PENDIENTE', '', 'RECUPERADO') 
            GROUP BY Hash, CI_Estudiante 
            HAVING COUNT(*) > 1
        """)
        grupos_hash = c.fetchall()
        
        eliminados_2 = 0
        for grupo in grupos_hash:
            hash_val = grupo.get('Hash') or grupo.get('hash')
            ci = grupo.get('CI_Estudiante') or grupo.get('ci_estudiante')

            c.execute("""
                SELECT id, Url_Archivo FROM Evidencias 
                WHERE Hash = %s AND CI_Estudiante = %s 
                ORDER BY id ASC
            """, (hash_val, ci))
            
            copias = c.fetchall()
            # original = copias[0] -> No necesitamos tocar el original
            
            # Borrar copias extra
            for copia in copias[1:]:
                # Solo borramos de la DB, NO de la nube (porque el original la usa)
                c.execute("DELETE FROM Evidencias WHERE id = %s", (copia['id'],))
                eliminados_2 += 1
        
        if eliminados_2 > 0: print(f"   ✨ Fase 2: {eliminados_2} duplicados exactos eliminados.")

        # =========================================================
        # FASE 3: ELIMINAR POR NOMBRE + ESTUDIANTE
        # =========================================================
        print("🔍 FASE 3: Buscando duplicados por nombre de archivo...")
        c.execute("SELECT id, CI_Estudiante, Url_Archivo FROM Evidencias")
        todas = c.fetchall()
        agrupados = {}
        
        for ev in todas:
            url = ev.get('Url_Archivo') or ev.get('url_archivo')
            cedula = ev.get('CI_Estudiante') or ev.get('ci_estudiante')
            if not url: continue
            
            filename = os.path.basename(url)
            # Limpiar prefijos numéricos para comparar nombres reales
            clean_name = re.sub(r'^\d+_', '', filename)
            
            # CLAVE ÚNICA: Cédula + Nombre Archivo (Para que no mezcle alumnos)
            clave = f"{cedula}|{clean_name}"
            
            if clave not in agrupados: agrupados[clave] = []
            agrupados[clave].append(ev)
            
        eliminados_3 = 0
        for clave, lista in agrupados.items():
            if len(lista) > 1:
                lista.sort(key=lambda x: x['id']) # El más antiguo se queda
                duplicados = lista[1:] 
                for dup in duplicados:
                    # Aquí borramos de DB. NO borramos de nube por precaución en start-up.
                    c.execute("DELETE FROM Evidencias WHERE id = %s", (dup['id'],))
                    eliminados_3 += 1

        if eliminados_3 > 0: print(f"   ✨ Fase 3: {eliminados_3} archivos eliminados por nombre.")

        # =========================================================
        # FASE 4: SINCRONIZACIÓN SEGURA (SOLO LECTURA / ACTUALIZAR PESO)
        # =========================================================
        print("☁️ FASE 4: Auditando existencia real en la nube...")
        conn.commit() 
        c.execute("SELECT id, Url_Archivo FROM Evidencias")
        evidencias = c.fetchall()
        
        actualizados_peso = 0
        fantasmas_detectados = 0
        
        for ev in evidencias:
            url = ev.get('Url_Archivo') or ev.get('url_archivo')
            ev_id = ev.get('id')
            
            if not url: continue

            # --- LÓGICA SEGURA: Por defecto NUNCA borramos ---
            debe_borrarse = False 
            peso_kb = 0
            
            # CASO A: Archivos en la Nube (S3/Backblaze)
            if "backblazeb2.com" in url or "s3" in url:
                if s3_client:
                    try:
                        parsed = urlparse(url)
                        key = parsed.path.lstrip('/')
                        
                        # Intentar obtener metadatos
                        meta = s3_client.head_object(Bucket=BUCKET_NAME, Key=key)
                        peso_kb = meta['ContentLength'] / 1024
                        
                        # Si existe, actualizamos peso
                        c.execute("UPDATE Evidencias SET Tamanio_KB = %s WHERE id = %s", (peso_kb, ev_id))
                        actualizados_peso += 1
                        
                    except Exception as e:
                        error_msg = str(e)
                        if "404" in error_msg or "Not Found" in error_msg:
                            print(f"⚠️ Alerta: Archivo no detectado en nube (404): {url}")
                            fantasmas_detectados += 1
                            debe_borrarse = False # MODO SEGURO: NO BORRAR
                        else:
                            pass

            # CASO B: Archivos Locales (Si es Railway prod, esto suele fallar, pero lo dejamos seguro)
            elif "/local/" in url:
                pass
            
            # Si activaras el borrado, iría aquí.
            if debe_borrarse:
                c.execute("DELETE FROM Evidencias WHERE id = %s", (ev_id,))
        
        conn.commit()
        print(f"✅ FASE 4 COMPLETADA: {actualizados_peso} pesos actualizados.")
        
        # Actualizar métricas finales
        try:
            stats = calcular_estadisticas_reales()
            fecha_hoy = ahora_ecuador().date().isoformat()
            
            conn_metricas = get_db_connection()
            c_met = conn_metricas.cursor()
            c_met.execute("""
                INSERT INTO Metricas_Sistema 
                (Fecha, Total_Usuarios, Total_Evidencias, Solicitudes_Pendientes, Almacenamiento_MB)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (Fecha) DO UPDATE SET
                Total_Usuarios = EXCLUDED.Total_Usuarios,
                Total_Evidencias = EXCLUDED.Total_Evidencias,
                Solicitudes_Pendientes = EXCLUDED.Solicitudes_Pendientes,
                Almacenamiento_MB = EXCLUDED.Almacenamiento_MB
            """, (fecha_hoy, stats.get("usuarios_activos",0), stats.get("total_evidencias",0), 
                  stats.get("solicitudes_pendientes",0), stats.get("almacenamiento_mb",0)))
            conn_metricas.commit()
            conn_metricas.close()
        except Exception as e:
            print(f"⚠️ Error menor actualizando métricas: {e}")
            
    except Exception as e:
        print(f"❌ Error general en limpieza startup: {e}")
    finally:
        if conn: conn.close()
        print(f"✅ MANTENIMIENTO TOTAL FINALIZADO.")

@app.post("/recuperar_evidencias_nube")
async def recuperar_evidencias_nube(background_tasks: BackgroundTasks):
    """
    Escanea TODO el bucket de Backblaze, encuentra los archivos que faltan en la BD
    y los restaura automáticamente (intentando usar IA para re-asignarlos).
    """
    def tarea_rescate():
        print("🚑 INICIANDO OPERACIÓN RESCATE DE EVIDENCIAS...")
        if not s3_client:
            print("❌ No hay conexión S3 para el rescate.")
            return

        conn = get_db_connection()
        c = conn.cursor() # <--- IMPORTANTE: Creamos el cursor aquí
        restaurados = 0
        
        try:
            # 1. Listar TODOS los objetos en la carpeta 'evidencias/' de la nube
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix='evidencias/')
            
            for page in pages:
                if 'Contents' not in page: continue
                
                for obj in page['Contents']:
                    key = obj['Key'] # Ejemplo: evidencias/123_foto.jpg
                    
                    # Ignoramos carpetas vacías
                    if key.endswith('/'): continue
                    
                    # Construimos la URL que debería tener
                    url_archivo = f"https://{BUCKET_NAME}.s3.us-east-005.backblazeb2.com/{key}"
                    
                    # 2. Verificar si ya existe en la BD (Usando el cursor 'c')
                    c.execute("SELECT id FROM Evidencias WHERE Url_Archivo = %s", (url_archivo,))
                    existe = c.fetchone()
                    
                    if not existe:
                        print(f"   📥 Recuperando: {key}...")
                        
                        # Descargar para análisis IA (si es imagen)
                        temp_path = None
                        ci_detectada = '9999999990' # Por defecto a bandeja recuperados
                        asignado_auto = 0
                        
                        try:
                            # Descargar archivo temporal
                            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                                s3_client.download_fileobj(BUCKET_NAME, key, tmp)
                                temp_path = tmp.name
                            
                            # Intentar reconocer rostros para re-asignar
                            if key.lower().endswith(('.jpg', '.jpeg', '.png', '.webp', '.avif')) and rekog:
                                try:
                                    rostros = identificar_varios_rostros_aws(temp_path)
                                    if rostros:
                                        # Si encuentra rostros, buscamos si son estudiantes
                                        for rostro in rostros:
                                            c.execute("SELECT CI FROM Usuarios WHERE CI=%s", (rostro,))
                                            u = c.fetchone()
                                            if u:
                                                ci_detectada = u['CI']
                                                asignado_auto = 1
                                                break
                                except: pass
                            
                            # Calcular Hash nuevo
                            nuevo_hash = calcular_hash(temp_path)
                            size_kb = obj['Size'] / 1024
                            
                            # Insertar en BD (Usando el cursor 'c')
                            tipo = 'video' if key.lower().endswith(('.mp4', '.avi')) else 'imagen'
                            c.execute("""
                                INSERT INTO Evidencias (CI_Estudiante, Url_Archivo, Hash, Estado, Tipo_Archivo, Tamanio_KB, Asignado_Automaticamente)
                                VALUES (%s, %s, %s, 1, %s, %s, %s)
                            """, (ci_detectada, url_archivo, nuevo_hash, tipo, size_kb, asignado_auto))
                            
                            restaurados += 1
                            
                            if temp_path and os.path.exists(temp_path): os.remove(temp_path)
                            
                        except Exception as e:
                            print(f"   ⚠️ Error parcial recuperando {key}: {e}")
                            # Intentamos insertar aunque sea sin IA
                            try:
                                c.execute("""
                                    INSERT INTO Evidencias (CI_Estudiante, Url_Archivo, Hash, Estado, Tipo_Archivo, Tamanio_KB, Asignado_Automaticamente)
                                    VALUES ('9999999990', %s, 'RECUPERADO', 1, 'desconocido', 0, 0)
                                """, (url_archivo,))
                                restaurados += 1
                            except: pass

            conn.commit()
            print(f"✅ OPERACIÓN RESCATE FINALIZADA: {restaurados} evidencias restauradas.")
            
            # Actualizar estadísticas (Usando el cursor 'c')
            try:
                stats = calcular_estadisticas_reales()
                fecha = ahora_ecuador().date().isoformat()
                c.execute("""
                    INSERT INTO Metricas_Sistema (Fecha, Total_Evidencias, Almacenamiento_MB) 
                    VALUES (%s, %s, %s)
                    ON CONFLICT (Fecha) DO UPDATE SET
                    Total_Evidencias = EXCLUDED.Total_Evidencias,
                    Almacenamiento_MB = EXCLUDED.Almacenamiento_MB
                """, (fecha, stats.get('total_evidencias',0), stats.get('almacenamiento_mb',0)))
                conn.commit()
            except Exception as e:
                print(f"⚠️ Error actualizando métricas tras rescate: {e}")

        except Exception as e:
            print(f"❌ Error crítico en rescate: {e}")
        finally:
            conn.close()

    background_tasks.add_task(tarea_rescate)
    return JSONResponse({"mensaje": "🚑 Rescate iniciado. Revisa los logs en 2 minutos."})

class ReasignarRequest(BaseModel):
    ids: str
    cedula_destino: str


    # --- AGREGA ESTE NUEVO ENDPOINT PARA RECUPERAR TUS DATOS ---

@app.post("/reasignar_evidencias")
async def reasignar_evidencias(datos: ReasignarRequest):
    """
    V2.0 - Reasignación Multi-Destino:
    - Permite enviar una lista de cédulas destino (separadas por comas).
    - Al primer destino le MUEVE el archivo.
    - A los siguientes destinos les crea una COPIA (Clon) en la base de datos.
    """
    try:
        if not datos.ids or not datos.cedula_destino:
            return JSONResponse({"error": "Faltan datos"})
            
        ids_evidencias = [id.strip() for id in datos.ids.split(',') if id.strip()]
        cedulas_destino = [ced.strip() for ced in datos.cedula_destino.split(',') if ced.strip()]
        
        if not ids_evidencias or not cedulas_destino:
             return JSONResponse({"error": "Selección inválida"})

        conn = get_db_connection()
        c = conn.cursor()
        
        movidos = 0
        clonados = 0
        
        # 1. Obtener datos originales de las evidencias antes de moverlas
        placeholders = ','.join(['%s'] * len(ids_evidencias))
        evidencias_originales = c.execute(f"SELECT * FROM Evidencias WHERE id IN ({placeholders})", ids_evidencias).fetchall()

        # 2. PROCESAR CADA EVIDENCIA
        for ev in evidencias_originales:
            # A) Mover al PRIMER estudiante de la lista (UPDATE)
            primer_destino = cedulas_destino[0]
            c.execute("UPDATE Evidencias SET CI_Estudiante = %s, Asignado_Automaticamente = 0 WHERE id = %s", (primer_destino, ev['id']))
            movidos += 1
            
            # B) Clonar para el RESTO de estudiantes (INSERT)
            if len(cedulas_destino) > 1:
                for otro_destino in cedulas_destino[1:]:
                    c.execute("""
                        INSERT INTO Evidencias (CI_Estudiante, Url_Archivo, Hash, Estado, Tipo_Archivo, Tamanio_KB, Asignado_Automaticamente)
                        VALUES (%s, %s, %s, %s, %s, %s, 0)
                    """, (otro_destino, ev['Url_Archivo'], ev['Hash'], ev['Estado'], ev['Tipo_Archivo'], ev['Tamanio_KB']))
                    clonados += 1

        conn.commit()
        conn.close()
        
        mensaje = f"✅ Archivos movidos a 1 estudiante."
        if clonados > 0:
            mensaje += f" Y se crearon copias para {len(cedulas_destino)-1} estudiantes más."
        
        return JSONResponse({"mensaje": mensaje})
        
    except Exception as e:
        return JSONResponse({"error": str(e)})

# --- ENDPOINT DE EMERGENCIA PARA CORREGIR ADMIN ---
@app.get("/reparar_admin")
async def reparar_admin():
    """Fuerza al usuario 9999999999 a ser Administrador (Tipo 0)"""
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Encriptamos la contraseña por defecto
        pass_admin_hash = get_password_hash('admin123')
        
        c.execute("SELECT * FROM Usuarios WHERE CI = '9999999999'")
        user = c.fetchone()
        
        if not user:
            # Crear de cero con contraseña encriptada
            c.execute("""
                INSERT INTO Usuarios (Nombre, Apellido, CI, Password, Tipo, Activo) 
                VALUES ('Admin', 'Sistema', '9999999999', %s, 0, 1)
            """, (pass_admin_hash,))
            mensaje = "Usuario Admin no existía. CREADO exitosamente."
        else:
            # Actualizar existente y resetear clave a admin123 (encriptada)
            c.execute("""
                UPDATE Usuarios 
                SET Tipo = 0, Activo = 1, Password = %s 
                WHERE CI = '9999999999'
            """, (pass_admin_hash,))
            mensaje = "Usuario 9999999999 actualizado: ES ADMIN y clave reseteada."
            
        conn.commit()
        conn.close()
        return JSONResponse({"status": "ok", "mensaje": mensaje})
        
    except Exception as e:
        return JSONResponse({"error": str(e)})

    # --- ENDPOINT PARA ACTUALIZAR LA TABLA USUARIOS (EJECUTAR UNA VEZ) ---
@app.get("/actualizar_tabla_usuarios")
async def actualizar_tabla_usuarios():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Agregamos TEMA a la lista de columnas nuevas
        comandos = [
            "ALTER TABLE Usuarios ADD COLUMN IF NOT EXISTS Email TEXT;",
            "ALTER TABLE Usuarios ADD COLUMN IF NOT EXISTS Telefono TEXT;",
            "ALTER TABLE Usuarios ADD COLUMN IF NOT EXISTS Fecha_Registro TIMESTAMP DEFAULT NOW();",
            "ALTER TABLE Usuarios ADD COLUMN IF NOT EXISTS Tema INTEGER DEFAULT 0;" # <--- ESTO ES LO NUEVO
        ]
        
        for cmd in comandos:
            c.execute(cmd)
            
        conn.commit()
        conn.close()
        return {"mensaje": "✅ Base de datos actualizada: Se agregó columna Tema."}
    except Exception as e:
        return {"error": str(e)}

        # --- PEGAR ESTO JUSTO DEBAJO DE 'actualizar_tabla_usuarios' ---

@app.get("/actualizar_tabla_solicitudes")
async def actualizar_tabla_solicitudes():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        # Creamos la columna Fecha_Visto si no existe
        c.execute("ALTER TABLE Solicitudes ADD COLUMN IF NOT EXISTS Fecha_Visto TIMESTAMP NULL;")
        conn.commit()
        conn.close()
        return {"mensaje": "✅ Tabla Solicitudes actualizada con Fecha_Visto"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/migrar_seguridad")
async def migrar_seguridad():
    """
    Encripta todas las contraseñas que están en texto plano.
    Ejecutar una sola vez.
    """
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)
        
        # 1. Obtener todos los usuarios
        c.execute("SELECT CI, Password FROM Usuarios")
        usuarios = c.fetchall()
        
        actualizados = 0
        
        for u in usuarios:
            cedula = u['ci'] # O u['CI'] dependiendo de tu base de datos
            password_actual = u['password'] # O u['Password']
            
            # 2. Verificar si ya está encriptada (Los hash de bcrypt empiezan con $2b$)
            if password_actual and not password_actual.startswith("$2b$"):
                # Si no empieza con $2b$, es texto plano. ¡A encriptar!
                nuevo_hash = get_password_hash(password_actual)
                
                # Guardamos la versión segura
                c.execute("UPDATE Usuarios SET Password = %s WHERE CI = %s", (nuevo_hash, cedula))
                actualizados += 1
        
        conn.commit()
        conn.close()
        
        return JSONResponse({
            "status": "ok", 
            "mensaje": f"Se han encriptado {actualizados} contraseñas antiguas exitosamente."
        })
        
    except Exception as e:
        return JSONResponse({"error": str(e)})

        # --- Endpoint para que el estudiante borre una solicitud ya leída ---
@app.delete("/confirmar_lectura_solicitud/{id_solicitud}")
async def confirmar_lectura(id_solicitud: int):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Borramos la solicitud permanentemente
        cursor.execute("DELETE FROM solicitudes WHERE id = %s", (id_solicitud,))
        conn.commit()
        
        cursor.close()
        conn.close()
        return {"status": "ok", "mensaje": "Solicitud eliminada para ahorrar espacio."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "mensaje": str(e)})
    
    # --- NUEVO: Limpieza masiva cuando el estudiante marca "Leído" ---
@app.post("/limpiar_notificaciones_resueltas")
async def limpiar_notificaciones_resueltas(cedula: str = Form(...)):
    """
    Borra de la BD todas las solicitudes que ya no son PENDIENTES.
    Esto libera espacio en Supabase garantizando que el usuario ya las vio.
    """
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Borramos solo las que están APROBADA o RECHAZADA (Dejamos las PENDIENTES)
        c.execute("""
            DELETE FROM Solicitudes 
            WHERE CI_Solicitante = %s AND Estado != 'PENDIENTE'
        """, (cedula,))
        
        filas_borradas = c.rowcount
        conn.commit()
        conn.close()
        
        return {"status": "ok", "mensaje": f"Se eliminaron {filas_borradas} notificaciones antiguas."}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "mensaje": str(e)})
    
@app.post("/agregar_foto_referencia")
async def agregar_foto_referencia(
    cedula: str = Form(...),
    foto: UploadFile = File(...)
):
    """
    Agrega o actualiza la foto de referencia de un usuario.
    - Sube la foto a Backblaze B2 (S3) con ACL pública.
    - Actualiza la columna 'Foto' en la tabla Usuarios.
    - Indexa el rostro en AWS Rekognition (si es estudiante).
    """
    temp_dir = None
    conn = None
    try:
        import re
        
        # 1. Validar usuario
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Error de conexión a la base de datos")
        c = conn.cursor(cursor_factory=RealDictCursor)
        c.execute("SELECT CI, Tipo FROM Usuarios WHERE CI = %s", (cedula,))
        usuario = c.fetchone()
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")

        # 2. Guardar archivo temporal con nombre sanitizado
        nombre_limpio = re.sub(r'[^\w\.\-]', '_', foto.filename)
        temp_dir = tempfile.mkdtemp()
        path = os.path.join(temp_dir, nombre_limpio)
        with open(path, "wb") as f:
            shutil.copyfileobj(foto.file, f)
        print(f"📸 Foto guardada temporalmente: {path}")

        # 3. Subir a Backblaze B2 (S3) - ¡OBLIGATORIO!
        if not s3_client:
            raise HTTPException(status_code=500, detail="Almacenamiento en nube no disponible.")
        
        timestamp = int(ahora_ecuador().timestamp())
        nombre_nube = f"perfiles/{cedula}_{timestamp}_{nombre_limpio}"
        s3_client.upload_file(
            path,
            BUCKET_NAME,
            nombre_nube,
            ExtraArgs={'ACL': 'public-read'}  # 🔑 ¡Esto es lo que hace que sea pública!
        )
        url_foto = f"https://{BUCKET_NAME}.s3.us-east-005.backblazeb2.com/{nombre_nube}"
        print(f"✅ Foto subida a S3: {url_foto}")

        # 4. Actualizar la foto en la base de datos
        c.execute("UPDATE Usuarios SET Foto = %s WHERE CI = %s", (url_foto, cedula))
        conn.commit()
        print(f"✅ Foto actualizada en BD para {cedula}")

        # 5. Indexar rostro en AWS Rekognition (si es estudiante)
        if rekog and usuario.get('Tipo') == 1:
            try:
                with open(path, 'rb') as image_file:
                    image_bytes = image_file.read()
                rekog.index_faces(
                    CollectionId=COLLECTION_ID,
                    Image={'Bytes': image_bytes},
                    ExternalImageId=cedula,
                    MaxFaces=1,
                    QualityFilter='AUTO'
                )
                print(f"✅ Rostro indexado en AWS para {cedula}")
            except Exception as e_rek:
                print(f"⚠️ Error indexando rostro en AWS: {e_rek}")
                # No detenemos la operación, solo advertimos

        # 6. Limpiar y responder
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

        registrar_auditoria(
            "AGREGAR_FOTO_REF",
            f"Admin actualizó foto de referencia de {cedula}",
            "Administrador"
        )

        return JSONResponse({
            "status": "ok",
            "mensaje": "Foto de referencia actualizada correctamente.",
            "url": url_foto
        })

    except HTTPException as http_exc:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        raise http_exc
    except Exception as e:
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        print(f"❌ Error en agregar_foto_referencia: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    import uvicorn
    import os
    
    # 1. Atrapamos el puerto dinámico de Railway, si no hay, usamos 8080
    puerto_railway = int(os.environ.get("PORT", 8080))
    
    print("=" * 60)
    print("🚀 SISTEMA EDUCATIVO DESPERTAR - BACKEND V7.0")
    print("=" * 60)
    print(f"🌍 Servidor forzado en Host: 0.0.0.0 | Puerto: {puerto_railway}")
    
    # 2. Obligamos a Uvicorn a respetar nuestra configuración
    uvicorn.run("main:app", host="0.0.0.0", port=puerto_railway)