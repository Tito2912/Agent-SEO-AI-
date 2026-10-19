"""Customer-facing states for search statistics, independent of provider transport."""
from typing import Any


def availability(payload: dict[str, Any], source: str) -> dict[str, Any]:
    label = "Google" if source == "gsc" else "Bing"
    reason = str(payload.get("reason") or "")
    if payload.get("ok"):
        return {"kind": "ready"}
    if reason == "disabled":
        return {"kind": "connect", "title": f"Activez le suivi {label} pour ce site",
                "detail": "Choisissez le compte autorisé à consulter les recherches de ce site.",
                "action": f"Configurer {label}"}
    if reason in {"missing_credentials", "not_connected", "oauth_not_connected", "missing_api_key",
                  "oauth_invalid_grant"}:
        return {"kind": "connect", "title": f"Connectez {label} pour ce site",
                "detail": "L’autorisation est absente ou a expiré. Connectez le compte qui gère ce site.",
                "action": f"Connecter {label}"}
    return {"kind": "unavailable", "title": f"Les données {label} sont indisponibles",
            "detail": "Le service n’a pas fourni de résultats utilisables. Réessayez ; si cela persiste, vérifiez que le compte autorisé a accès à ce site.",
            "action": "Vérifier l’accès à ce site"}
