import requests

url = "https://api.msci.com/esg/data/v2.0/healthcheck"
payload={}
headers = {
  'Client ID': 'a568Wa48TM3xzfeOT8xxe3V5VJzo4Mfb',
  'Client Secret': 'S1HM7CrxsnbUMRTUkn8o8-t-_OEYnSfXLyaze0IpgX1vPDweBW35wHzmidyvWxd6'
}
response = requests.request("GET", url, headers=headers, data=payload)
print(response.text)