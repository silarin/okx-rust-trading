# Security policy

## Credentials

Do not commit OKX API keys, secrets, passphrases, cookies, tokens, private keys, certificates, local operator profiles, or generated account/order artifacts.

Use `OKX_API_KEY`, `OKX_API_SECRET`, and `OKX_API_PASSPHRASE`, or their `_FILE` variants, at runtime. Restrict API keys to the minimum required permissions, use IP restrictions where available, keep withdrawal permissions disabled, and prefer OKX Demo Trading Services while developing or validating changes.

If a credential may have entered a commit, log, artifact, issue, or pull request, revoke or rotate it immediately. Removing a value from the current tree does not remove it from Git history or caches.

## Reporting a vulnerability

Please report security vulnerabilities privately through [GitHub's private vulnerability reporting](https://github.com/silarin/okx-rust-trading/security/advisories/new). Do not open a public issue containing exploit details, credentials, account identifiers, or sensitive logs.

Include the affected revision, impact, reproduction conditions, and a minimal sanitized example when possible. Never test a report against accounts or systems you do not own or have explicit permission to use.
