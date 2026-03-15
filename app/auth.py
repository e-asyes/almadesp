import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from functools import lru_cache
from typing import Optional
from pydantic import BaseModel

# === Keycloak config ===
ISSUER = "https://hercules.ajleon.cl/auth/realms/clientes-prod"
JWKS_URI = (
    "https://hercules.ajleon.cl/auth/realms/clientes-prod/"
    "protocol/openid-connect/certs"
)

# Simple Bearer token scheme - just paste your token in Swagger UI
bearer_scheme = HTTPBearer()


class TokenPayload(BaseModel):
    """Validated token payload with common Keycloak claims."""
    sub: str  # Subject (user ID)
    preferred_username: Optional[str] = None
    email: Optional[str] = None
    name: Optional[str] = None
    empresas: Optional[list[dict]] = None
    realm_access: Optional[dict] = None
    resource_access: Optional[dict] = None


@lru_cache(maxsize=1)
def get_jwks() -> dict:
    """Fetch and cache JWKS from Keycloak."""
    response = httpx.get(JWKS_URI, timeout=10.0)
    response.raise_for_status()
    return response.json()


def get_public_key(token: str) -> dict:
    """Get the public key that matches the token's kid."""
    jwks = get_jwks()
    unverified_header = jwt.get_unverified_header(token)
    kid = unverified_header.get("kid")

    for key in jwks.get("keys", []):
        if key.get("kid") == kid:
            return key

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Unable to find matching key in JWKS",
        headers={"WWW-Authenticate": "Bearer"},
    )


def verify_token(token: str) -> TokenPayload:
    """Verify and decode a JWT token from Keycloak."""
    try:
        public_key = get_public_key(token)

        payload = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            issuer=ISSUER,
            options={"verify_aud": False},  # Keycloak audience can vary
        )

        return TokenPayload(**payload)

    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token validation failed: {str(e)}",
            headers={"WWW-Authenticate": "Bearer"},
        )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme)
) -> TokenPayload:
    """FastAPI dependency to get the current authenticated user."""
    return verify_token(credentials.credentials)


def require_role(required_role: str):
    """
    Dependency factory to require a specific realm role.

    Usage:
        @router.get("/admin", dependencies=[Depends(require_role("admin"))])
        def admin_endpoint():
            ...
    """
    async def role_checker(user: TokenPayload = Depends(get_current_user)):
        roles = user.realm_access.get("roles", []) if user.realm_access else []
        if required_role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{required_role}' required",
            )
        return user
    return role_checker
