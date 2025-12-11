"""
加密工具模块 - 用于API密钥的加密和解密
"""
import os
from pathlib import Path
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64

# 密钥文件路径
KEY_FILE = Path(__file__).parent / ".encryption_key"

def get_or_create_key():
    """获取或创建加密密钥"""
    if KEY_FILE.exists():
        # 读取现有密钥
        with open(KEY_FILE, 'rb') as f:
            return f.read()
    else:
        # 生成新密钥
        key = Fernet.generate_key()
        # 保存密钥到文件（只读权限）
        with open(KEY_FILE, 'wb') as f:
            f.write(key)
        # 设置文件权限（Windows上可能无效，但保留以便跨平台）
        try:
            os.chmod(KEY_FILE, 0o600)  # 只有所有者可读写
        except:
            pass
        return key

def get_cipher():
    """获取加密/解密器"""
    key = get_or_create_key()
    return Fernet(key)

def encrypt_api_key(api_key: str) -> str:
    """加密API密钥
    
    Args:
        api_key: 原始API密钥
        
    Returns:
        加密后的API密钥（base64编码字符串）
    """
    if not api_key:
        return None
    
    try:
        cipher = get_cipher()
        encrypted = cipher.encrypt(api_key.encode('utf-8'))
        return base64.b64encode(encrypted).decode('utf-8')
    except Exception as e:
        print(f"加密API密钥失败: {e}")
        return api_key  # 如果加密失败，返回原始值

def decrypt_api_key(encrypted_api_key: str) -> str:
    """解密API密钥
    
    Args:
        encrypted_api_key: 加密后的API密钥（base64编码字符串）
        
    Returns:
        解密后的原始API密钥
    """
    if not encrypted_api_key:
        return None
    
    # 检查是否是加密格式（base64编码的Fernet token）
    try:
        # 尝试base64解码
        encrypted_bytes = base64.b64decode(encrypted_api_key.encode('utf-8'))
        cipher = get_cipher()
        decrypted = cipher.decrypt(encrypted_bytes)
        return decrypted.decode('utf-8')
    except Exception:
        # 如果不是加密格式，可能是旧配置的明文，直接返回
        return encrypted_api_key

def is_encrypted(api_key: str) -> bool:
    """检查API密钥是否已加密
    
    Args:
        api_key: API密钥字符串
        
    Returns:
        如果是加密格式返回True，否则返回False
    """
    if not api_key:
        return False
    
    try:
        # 尝试base64解码
        base64.b64decode(api_key.encode('utf-8'))
        # 如果能解码，再尝试Fernet解密（不实际解密，只检查格式）
        encrypted_bytes = base64.b64decode(api_key.encode('utf-8'))
        # Fernet token的长度应该是固定的
        if len(encrypted_bytes) >= 57:  # Fernet token的最小长度
            return True
        return False
    except:
        return False

