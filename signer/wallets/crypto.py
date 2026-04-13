from __future__ import annotations

import os
from base64 import b64decode
from base64 import b64encode
from base64 import urlsafe_b64encode

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# 每次加密使用随机 16 字节盐值，存储格式：base64(salt_16B + fernet_token)。
_SALT_SIZE = 16
_KDF_ITERATIONS = 600_000


class AESCipher:
    """signer 自己持有的对称加密工具，只用于本地助记词加解密。

    密钥派生使用 PBKDF2-HMAC-SHA256（60 万次迭代 + 随机盐），
    防止低熵密码短语被暴力枚举。
    """

    def __init__(self, key: str):
        self._key_bytes = key.encode("utf-8")

    @staticmethod
    def _derive_fernet_key(key_bytes: bytes, salt: bytes) -> bytes:
        # PBKDF2 派生 32 字节密钥，再 base64 编码为 Fernet 所需的 url-safe 格式。
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=_KDF_ITERATIONS,
        )
        return urlsafe_b64encode(kdf.derive(key_bytes))

    def encrypt(self, message: str) -> str:
        salt = os.urandom(_SALT_SIZE)
        fernet = Fernet(self._derive_fernet_key(self._key_bytes, salt))
        token = fernet.encrypt(message.encode("utf-8"))
        # 密文格式：base64(salt + fernet_token)，解密时先取出盐值再派生密钥。
        return b64encode(salt + token).decode("utf-8")

    def decrypt(self, message: str) -> str:
        raw = b64decode(message.encode("utf-8"))
        salt = raw[:_SALT_SIZE]
        token = raw[_SALT_SIZE:]
        fernet = Fernet(self._derive_fernet_key(self._key_bytes, salt))
        return fernet.decrypt(token).decode("utf-8")
