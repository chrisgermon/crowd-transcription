"""Active Directory authentication via LDAP with local fallback."""

import hashlib
import logging

from ldap3 import Connection, Server, SIMPLE, SUBTREE

logger = logging.getLogger(__name__)

# Default AD settings (used if not configured in settings)
_DEFAULT_AD_SERVER = "10.17.10.10"
_DEFAULT_AD_DOMAIN = "images.local"

# Fallback local user — always works even if AD is unreachable
_FALLBACK_USER = "chris"
_FALLBACK_HASH = hashlib.sha256(b"Leftfoot6").hexdigest()


def _get_ad_settings() -> tuple[str, str, str]:
    """Return (server, domain, base_dn) from config store or defaults."""
    try:
        from crowdtrans.config_store import get_config_store
        store = get_config_store()
        server = store.get_global("ad_server") or _DEFAULT_AD_SERVER
        domain = store.get_global("ad_domain") or _DEFAULT_AD_DOMAIN
    except Exception:
        server = _DEFAULT_AD_SERVER
        domain = _DEFAULT_AD_DOMAIN

    # Derive base DN from domain: "images.local" -> "DC=images,DC=local"
    base_dn = ",".join(f"DC={part}" for part in domain.split("."))
    return server, domain, base_dn


def authenticate(username: str, password: str) -> dict | None:
    """Authenticate a user against Active Directory, with local fallback.

    Returns a dict with user details on success, or None on failure.
    """
    if not username or not password:
        return None

    # Check fallback local user first
    if username.lower() == _FALLBACK_USER:
        if hashlib.sha256(password.encode()).hexdigest() == _FALLBACK_HASH:
            logger.info("Local fallback auth success for %s", username)
            return {
                "username": username,
                "display_name": "Chris (Local)",
                "email": "",
                "groups": [],
            }
        # Don't fall through to AD for the fallback user
        logger.info("Local fallback auth failed for %s", username)
        return None

    # Try Active Directory
    ad_server, ad_domain, ad_base_dn = _get_ad_settings()
    upn = f"{username}@{ad_domain}"

    server = Server(ad_server, port=389, use_ssl=False, get_info=None)
    try:
        conn = Connection(
            server,
            user=upn,
            password=password,
            authentication=SIMPLE,
            raise_exceptions=False,
            read_only=True,
            receive_timeout=10,
        )
        if not conn.bind():
            logger.info("AD auth failed for %s: %s", username, conn.result.get("description", ""))
            return None

        # Search for user details
        conn.search(
            search_base=ad_base_dn,
            search_filter=f"(&(objectClass=user)(sAMAccountName={_escape_ldap(username)}))",
            search_scope=SUBTREE,
            attributes=["displayName", "mail", "sAMAccountName", "memberOf"],
        )

        user_info = {"username": username}
        if conn.entries:
            entry = conn.entries[0]
            user_info["display_name"] = str(entry.displayName) if entry.displayName else username
            user_info["email"] = str(entry.mail) if entry.mail else ""
            user_info["groups"] = [str(g) for g in entry.memberOf] if entry.memberOf else []
        else:
            user_info["display_name"] = username
            user_info["email"] = ""
            user_info["groups"] = []

        conn.unbind()
        logger.info("AD auth success for %s (%s)", username, user_info.get("display_name"))
        return user_info

    except Exception:
        logger.exception("AD connection error for %s", username)
        return None


def _escape_ldap(value: str) -> str:
    """Escape special characters for LDAP filter values."""
    replacements = {
        "\\": "\\5c",
        "*": "\\2a",
        "(": "\\28",
        ")": "\\29",
        "\x00": "\\00",
    }
    for char, escaped in replacements.items():
        value = value.replace(char, escaped)
    return value
