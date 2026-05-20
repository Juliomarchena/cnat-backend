import os
import logging
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

logger = logging.getLogger("cnat")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
JWKS_URL = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"

security = HTTPBearer()
_jwks: dict | None = None


async def load_jwks():
    """Carga las claves públicas de Supabase al arrancar la app."""
    global _jwks
    import httpx
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(JWKS_URL)
            r.raise_for_status()
            _jwks = r.json()
        logger.info(f"JWKS cargado: {len(_jwks.get('keys', []))} clave(s) desde Supabase")
    except Exception as e:
        logger.error(f"Error cargando JWKS: {e}")


def _get_signing_key(token: str) -> dict:
    if not _jwks or not _jwks.get("keys"):
        raise HTTPException(status_code=503, detail="JWKS no disponible")
    try:
        header = jwt.get_unverified_header(token)
        kid = header.get("kid")
        for key in _jwks["keys"]:
            if key.get("kid") == kid:
                return key
        return _jwks["keys"][0]
    except Exception:
        return _jwks["keys"][0]


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    key = _get_signing_key(token)
    try:
        payload = jwt.decode(
            token,
            key,
            algorithms=["ES256", "HS256"],
            audience="authenticated",
        )
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalido o expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_current_user(payload: dict = Depends(verify_token)):
    user_id = payload.get("sub")
    email = payload.get("email")
    role = payload.get("user_metadata", {}).get("role", "readonly")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token sin usuario")
    return {"id": user_id, "email": email, "role": role}


def require_admin(user: dict = Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Se requiere rol admin")
    return user
