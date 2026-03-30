import time
import base64
import requests
import json
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5

class AuthUtil:
    def __init__(self, base_url, lob, public_key_str):
        self.base_url = base_url
        self.lob = lob
        self.public_key_str = public_key_str

    def _encrypt_password(self, password):
        """
        Encrypts password using RSA PKCS1 v1.5 with a timestamp nonce.
        Format: timestamp_ms:password
        """
        try:
            # 1. Current timestamp in milliseconds
            timestamp = int(time.time() * 1000)
            
            # 2. Construct payload
            raw_payload = f"{timestamp}:{password}"
            
            # 3. Import Key and Encrypt
            key = RSA.import_key(self.public_key_str)
            cipher = PKCS1_v1_5.new(key)
            encrypted_bytes = cipher.encrypt(raw_payload.encode('utf-8'))
            
            # 4. Return Base64 string
            return base64.b64encode(encrypted_bytes).decode('utf-8')
        except Exception as e:
            raise Exception(f"Encryption failed: {str(e)}")

    def generate_token(self, login_id, password):
        """
        Calls the /signin API to retrieve a Bearer token.
        """
        encrypted_pass = self._encrypt_password(password)
        
        endpoint = f"/signin?lob={self.lob}&expiry=unlimited"
        url = f"{self.base_url}{endpoint}"
        
        headers = {
            'lob': self.lob,
            'Content-Type': 'application/json;charset=UTF-8',
            'User-Agent': 'Python/KPI-Automation'
        }
        
        payload = {
            "loginId": login_id,
            "password": encrypted_pass,
            "unlimitedExpiry": True,
            "lob": self.lob
        }
        
        # Debug print (Optional)
        print(f"Logging in user {login_id}...")
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                token = data.get("token") or data.get("accessToken")
                if not token:
                    raise Exception("Token field not found in response")
                    
                # Ensure it has 'Bearer ' prefix
                if not token.startswith("Bearer "):
                    return f"Bearer {token}"
                return token
            else:
                raise Exception(f"Login failed [{response.status_code}]: {response.text}")
                
        except Exception as e:
            raise Exception(f"Auth Error: {str(e)}")