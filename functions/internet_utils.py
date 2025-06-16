import requests

def get_external_ip():
        try:
            response = requests.get('https://api.ipify.org?format=text', timeout=5) 
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Could not get external IP: {e}")
            return None
