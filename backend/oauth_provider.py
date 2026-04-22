"""
OAuth 2.1 provider for Nocturne — single-user, self-contained AS.

Enabled only when NOCTURNE_AUTH_PASSWORD is set.
Based on MCP SDK's simple-auth example.
"""

import os
import secrets
import time
from typing import Any

from pydantic import AnyHttpUrl
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    OAuthAuthorizationServerProvider,
    RefreshToken,
    construct_redirect_uri,
)
from mcp.server.auth.routes import create_auth_routes, cors_middleware
from mcp.server.auth.settings import AuthSettings, ClientRegistrationOptions
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken


class NocturneOAuthProvider(
    OAuthAuthorizationServerProvider[AuthorizationCode, RefreshToken, AccessToken]
):
    def __init__(self, server_url: str, auth_password: str):
        self.server_url = server_url.rstrip("/")
        self.auth_password = auth_password
        self.clients: dict[str, OAuthClientInformationFull] = {}
        self.auth_codes: dict[str, AuthorizationCode] = {}
        self.tokens: dict[str, AccessToken] = {}
        self.state_mapping: dict[str, dict[str, str | None]] = {}

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        return self.clients.get(client_id)

    async def register_client(self, client_info: OAuthClientInformationFull):
        if not client_info.client_id:
            raise ValueError("No client_id provided")
        self.clients[client_info.client_id] = client_info

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        state = params.state or secrets.token_hex(16)
        self.state_mapping[state] = {
            "redirect_uri": str(params.redirect_uri),
            "code_challenge": params.code_challenge,
            "redirect_uri_provided_explicitly": str(
                params.redirect_uri_provided_explicitly
            ),
            "client_id": client.client_id,
            "resource": params.resource,
        }
        return f"{self.server_url}/login?state={state}&client_id={client.client_id}"

    async def get_login_page(self, state: str) -> HTMLResponse:
        if not state:
            raise HTTPException(400, "Missing state parameter")
        return HTMLResponse(
            content=f"""<!DOCTYPE html>
<html>
<head>
    <title>Nocturne — Sign In</title>
    <style>
        body {{ font-family: system-ui, sans-serif; max-width: 400px; margin: 80px auto; padding: 20px; background: #0d1117; color: #c9d1d9; }}
        h2 {{ color: #58a6ff; }}
        input {{ width: 100%; padding: 10px; margin: 6px 0 16px; background: #161b22; border: 1px solid #30363d; color: #c9d1d9; border-radius: 6px; box-sizing: border-box; }}
        button {{ background: #238636; color: #fff; padding: 10px 20px; border: none; border-radius: 6px; cursor: pointer; font-size: 14px; }}
        button:hover {{ background: #2ea043; }}
    </style>
</head>
<body>
    <h2>Nocturne Memory Server</h2>
    <p>Sign in to grant access to your memories.</p>
    <form action="{self.server_url}/login/callback" method="post">
        <input type="hidden" name="state" value="{state}">
        <label>Password:</label>
        <input type="password" name="password" required autofocus>
        <button type="submit">Sign In</button>
    </form>
</body>
</html>"""
        )

    async def handle_login_callback(self, request: Request) -> Response:
        form = await request.form()
        password = form.get("password")
        state = form.get("state")
        if not password or not state:
            raise HTTPException(400, "Missing password or state")
        if not isinstance(password, str) or not isinstance(state, str):
            raise HTTPException(400, "Invalid parameter types")

        state_data = self.state_mapping.get(state)
        if not state_data:
            raise HTTPException(400, "Invalid state parameter")

        redirect_uri = state_data["redirect_uri"]
        code_challenge = state_data["code_challenge"]
        redirect_uri_provided_explicitly = (
            state_data["redirect_uri_provided_explicitly"] == "True"
        )
        client_id = state_data["client_id"]
        resource = state_data.get("resource")

        assert redirect_uri is not None
        assert code_challenge is not None
        assert client_id is not None

        if not secrets.compare_digest(password, self.auth_password):
            raise HTTPException(401, "Invalid credentials")

        new_code = f"mcp_{secrets.token_hex(16)}"
        auth_code = AuthorizationCode(
            code=new_code,
            client_id=client_id,
            redirect_uri=AnyHttpUrl(redirect_uri),
            redirect_uri_provided_explicitly=redirect_uri_provided_explicitly,
            expires_at=time.time() + 300,
            scopes=["memory"],
            code_challenge=code_challenge,
            resource=resource,
        )
        self.auth_codes[new_code] = auth_code
        del self.state_mapping[state]

        return RedirectResponse(
            url=construct_redirect_uri(redirect_uri, code=new_code, state=state),
            status_code=302,
        )

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        return self.auth_codes.get(authorization_code)

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        if authorization_code.code not in self.auth_codes:
            raise ValueError("Invalid authorization code")
        if not client.client_id:
            raise ValueError("No client_id provided")

        mcp_token = f"mcp_{secrets.token_hex(32)}"
        self.tokens[mcp_token] = AccessToken(
            token=mcp_token,
            client_id=client.client_id,
            scopes=authorization_code.scopes,
            expires_at=int(time.time()) + 86400,  # 24 hours
            resource=authorization_code.resource,
        )
        del self.auth_codes[authorization_code.code]

        return OAuthToken(
            access_token=mcp_token,
            token_type="Bearer",
            expires_in=86400,
            scope=" ".join(authorization_code.scopes),
        )

    async def load_access_token(self, token: str) -> AccessToken | None:
        access_token = self.tokens.get(token)
        if not access_token:
            return None
        if access_token.expires_at and access_token.expires_at < time.time():
            del self.tokens[token]
            return None
        return access_token

    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        return None

    async def exchange_refresh_token(
        self,
        client: OAuthClientInformationFull,
        refresh_token: RefreshToken,
        scopes: list[str],
    ) -> OAuthToken:
        raise NotImplementedError("Refresh tokens not supported")

    async def revoke_token(
        self, token: str, token_type_hint: str | None = None
    ) -> None:
        if token in self.tokens:
            del self.tokens[token]


# ---------------------------------------------------------------------------
# Factory + route builder
# ---------------------------------------------------------------------------

_provider_instance: NocturneOAuthProvider | None = None


def create_oauth_provider() -> NocturneOAuthProvider | None:
    """Return a provider if NOCTURNE_AUTH_PASSWORD is set, else None."""
    global _provider_instance
    if _provider_instance is not None:
        return _provider_instance

    password = os.environ.get("NOCTURNE_AUTH_PASSWORD", "")
    server_url = os.environ.get("NOCTURNE_SERVER_URL", "")
    if not password or not server_url:
        return None

    _provider_instance = NocturneOAuthProvider(server_url, password)
    return _provider_instance


def get_oauth_routes(provider: NocturneOAuthProvider) -> list[Route]:
    """Build Starlette routes for OAuth endpoints."""
    server_url = provider.server_url

    auth_settings = AuthSettings(
        issuer_url=AnyHttpUrl(server_url),
        client_registration_options=ClientRegistrationOptions(
            enabled=True,
            valid_scopes=["memory"],
            default_scopes=["memory"],
        ),
        required_scopes=["memory"],
        resource_server_url=None,
    )

    routes = list(
        create_auth_routes(
            provider=provider,
            issuer_url=auth_settings.issuer_url,
            client_registration_options=auth_settings.client_registration_options,
            revocation_options=auth_settings.revocation_options,
        )
    )

    # Login page
    async def login_page(request: Request) -> Response:
        state = request.query_params.get("state")
        if not state:
            raise HTTPException(400, "Missing state")
        return await provider.get_login_page(state)

    routes.append(Route("/login", endpoint=login_page, methods=["GET"]))

    # Login callback
    async def login_callback(request: Request) -> Response:
        return await provider.handle_login_callback(request)

    routes.append(
        Route("/login/callback", endpoint=login_callback, methods=["POST"])
    )

    # RFC 9728: Protected Resource Metadata
    async def protected_resource_metadata(request: Request) -> Response:
        return JSONResponse(
            {
                "resource": server_url,
                "authorization_servers": [server_url],
                "bearer_methods_supported": ["header"],
                "scopes_supported": ["memory"],
            }
        )

    routes.append(
        Route(
            "/.well-known/oauth-protected-resource",
            endpoint=protected_resource_metadata,
            methods=["GET"],
        )
    )

    return routes
