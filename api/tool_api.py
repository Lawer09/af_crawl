from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form
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

from services.otp_service import get_2fa_code_by_pid
from services.otp_service import save_2fa_secret_from_qr

@router.get("/2fa/pid/{pid}")
def get_pid_2fa_code(pid: str):
    """
    Get 2FA code for a specific user (PID).
    """
    try:
        code = get_2fa_code_by_pid(pid)
        return {"status": "success", "code": code}
    except ValueError as e:
        # Business logic error (e.g., pid not found, key missing)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error generating 2FA code for pid {pid}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/2fa/qr")
def upload_qr_and_set_secret(
    pid: str = Form(..., description="用户 PID"),
    file: UploadFile = File(..., description="二维码图片文件")
):
    """上传二维码图片，识别 otpauth 文本并将 secret 写入 af_user 表。

    返回：{"status": "success", "pid": pid, "secret": secret}
    错误：400 - 业务错误（如 PID 不存在、未识别到二维码、缺少 secret 参数）
    """
    try:
        content = file.file.read()
        result = save_2fa_secret_from_qr(pid, content)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception(f"Error processing QR for pid {pid}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
