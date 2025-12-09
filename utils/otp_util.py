import hmac
import base64
import struct
import hashlib
import time

def get_totp_token(secret: str, interval: int = 30) -> str:
    """
    Generate a Time-based One-Time Password (TOTP) from a secret key.
    
    Args:
        secret (str): The base32 encoded secret key (e.g., from Google Authenticator).
        interval (int): The time step in seconds (default: 30).
        
    Returns:
        str: The 6-digit TOTP code.
    """
    if not secret:
        raise ValueError("Secret key cannot be empty")

    # Normalize secret: remove spaces and convert to uppercase
    secret = secret.replace(" ", "").upper()
    
    # Ensure secret length is valid for base32 decoding (padding if necessary)
    # Base32 requires length to be a multiple of 8
    missing_padding = len(secret) % 8
    if missing_padding != 0:
        secret += '=' * (8 - missing_padding)
        
    try:
        key = base64.b32decode(secret, casefold=True)
    except Exception as e:
        raise ValueError(f"Invalid base32 secret: {e}")

    # Get the current time interval
    # TOTP uses the number of 30-second intervals since the Unix epoch
    msg = struct.pack(">Q", int(time.time()) // interval)
    
    # Generate HMAC-SHA1
    h = hmac.new(key, msg, hashlib.sha1).digest()
    
    # Dynamic truncation
    o = h[19] & 15
    # Extract 4 bytes starting at offset o, mask the MSB, and take modulo 10^6
    h = (struct.unpack(">I", h[o:o+4])[0] & 0x7fffffff) % 1000000
    
    # Return as zero-padded string
    return '{:06d}'.format(h)
