import requests
import time
import json

def get_ngrok_url():
    """
    Get the public ngrok URL from the ngrok API.
    Returns the HTTPS URL if available, otherwise HTTP URL.
    """
    try:
        # Try to get tunnels from ngrok API
        response = requests.get("http://127.0.0.1:4040/api/tunnels", timeout=5)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        tunnels = data.get("tunnels", [])
        
        if not tunnels:
            print("No tunnels found in ngrok API response")
            return None
        
        # Print all available tunnels for debugging
        print(f"Found {len(tunnels)} tunnel(s):")
        for i, tunnel in enumerate(tunnels):
            print(f"  Tunnel {i+1}: {tunnel.get('public_url', 'No URL')} ({tunnel.get('proto', 'Unknown protocol')})")
        
        # Prefer HTTPS over HTTP
        for tunnel in tunnels:
            if tunnel.get("proto") == "https":
                url = tunnel.get("public_url")
                if url:
                    print(f"Using HTTPS tunnel: {url}")
                    return url
        
        # If no HTTPS, use HTTP
        for tunnel in tunnels:
            if tunnel.get("proto") == "http":
                url = tunnel.get("public_url")
                if url:
                    print(f"Using HTTP tunnel: {url}")
                    return url
        
        print("No valid tunnel URLs found")
        return None
        
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to ngrok API at http://127.0.0.1:4040")
        print("Make sure ngrok is running with: ngrok http 8000")
        return None
    except requests.exceptions.Timeout:
        print("Error: Timeout connecting to ngrok API")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error making request to ngrok API: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing ngrok API response: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error getting ngrok URL: {e}")
        return None

def test_ngrok_connection():
    """
    Test function to check if ngrok is working properly.
    """
    print("Testing ngrok connection...")
    url = get_ngrok_url()
    if url:
        print(f"✅ Ngrok URL retrieved successfully: {url}")
        return url
    else:
        print("❌ Failed to get ngrok URL")
        return None

if __name__ == "__main__":
    # Test the function when run directly
    test_ngrok_connection()
