import os
from pathlib import Path
from dotenv import load_dotenv

# 1. Ver dónde estamos parados realmente
print("--- DIAGNÓSTICO DE ENTORNO ---")
print(f"1. Directorio de ejecución actual: {Path.cwd()}")

# 2. Buscar el archivo .env en la raíz
env_path = Path.cwd() / ".env"
print(f"2. Buscando .env en: {env_path}")
print(f"3. ¿El archivo .env existe físicamente?: {env_path.exists()}")

if env_path.exists():
    print(f"4. Tamaño del archivo .env: {env_path.stat().st_size} bytes")

    # Intentar leerlo a la antigüita con dotenv
    load_dotenv(dotenv_path=env_path)
    key_directa = os.getenv("CONGRESS_API_KEY")
    print(f"5. ¿os.getenv puede leer CONGRESS_API_KEY?: {key_directa is not None}")
    if key_directa:
        # Mostramos solo los primeros caracteres por seguridad
        print(f"   -> Valor detectado (primeros 4 caracteres): {key_directa[:4]}...")
else:
    print("❌ El archivo .env NO está en este directorio.")
