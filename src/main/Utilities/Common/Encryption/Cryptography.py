import RC2
import base64
import re
from array import array
import array
import ast


key = ast.literal_eval(dbutils.secrets.get("<enter keyvault name>","<enter secret name>"))
IV  = ast.literal_eval(dbutils.secrets.get("<enter keyvault name>","<enter secret name>"))

class cipher():
  
  def __init__(self,val,key=key,IV=IV):
    self.key = key
    self.iv  = IV
    self.val = val
  
  @property
  def encrypt(self)->str:
      key                     = bytearray(self.key)
      iv                      = bytearray(self.iv)
      val                     = bytearray(val, 'utf-8')
      rc2                     = RC2(key)
      bytearry                = rc2.encrypt(self.val, MODE_CBC, IV=IV)
      byte                    = bytes(bytearry)
      base64str               = base64.b64encode(byte)
      return base64str
    
  @property
  def decrypt(self)->dict:
    try:
        key                     = bytearray(self.key)
        iv                      = bytearray(self.iv)
        rc2                     = RC2(key)
        b64decoded              = base64.b64decode(self.val)
        decrypted_raw           = rc2.decrypt(b64decoded, MODE_CBC, IV=IV)
        return decrypted_raw
      
    except Exception as e:
      print('Error found:', e)
      exit -1
