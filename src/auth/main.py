"""Module for doing first time OAuth flow"""
import webbrowser
import http.server
import socketserver
import urllib.parse
import secrets
import string
import os

CLIENT_ID = os.getenv("CLIENT_ID", "")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("REDIRECT_URI", "")
TENANT_ID = os.getenv("TENANT_ID", "")
scopes = ["https://ads.microsoft.com/msads.manage", "offline_access"]

def generate_state(length=30) -> str:
    """Generates a random state string."""
    alphabet = string.ascii_letters + string.digits
    state = ''.join(secrets.choice(alphabet) for i in range(length))
    return state

def get_authorization_url(state):
    authorization_url = (
        "https://login.microsoftonline.com/common/oauth2/v2.0/authorize?"
        f"client_id={CLIENT_ID}&"
        f"scope={urllib.parse.quote(' '.join(scopes))}&"
        "response_type=code&"
        f"redirect_uri={urllib.parse.quote(REDIRECT_URI)}&"
        f"state={state}&prompt=login"
    )
    return authorization_url

def start_local_server() -> None:
    class RequestHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed_url = urllib.parse.urlparse(self.path)
            query_params = urllib.parse.parse_qs(parsed_url.query)
            if "code" in query_params:
                authorization_code = query_params["code"][0]
                self.server.authorization_code = authorization_code
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"Authentication successful! You can close this window.")
            else:
                super().do_GET()

    with socketserver.TCPServer(("localhost", 8080), RequestHandler) as httpd:
        httpd.authorization_code = None
        httpd.state = generate_state()
        webbrowser.open(get_authorization_url(httpd.state))
        httpd.handle_request()
        return httpd.authorization_code