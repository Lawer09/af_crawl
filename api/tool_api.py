from fastapi import APIRouter, HTTPException, Query
from utils import get_totp_token
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/tool", tags=["tool"])

@router.get("/2fa")
def get_2fa_code(secret: str = Query(..., description="Google Authenticator secret key")):
    """
    Generate 2FA code (TOTP) from the provided secret key.
    """
    try:
        code = get_totp_token(secret)
        return {"status": "success", "code": code}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error generating 2FA code: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
