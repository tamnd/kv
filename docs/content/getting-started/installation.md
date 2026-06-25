---
title: "Installation"
description: "Add kv to a Go program with go get, or install the kv command-line tool from Go, Homebrew, Scoop, a release archive, a Linux package, or the container image."
weight: 20
---

kv is both a library you import and a single-binary command-line tool. The library is one `go get`; the CLI ships through every channel below.

## As a library

```bash
go get github.com/tamnd/kv@latest
```

```go
import "github.com/tamnd/kv"
```

That is the whole dependency. The module pulls in nothing outside the Go standard library, so it adds no transitive packages to your build. kv requires Go 1.23 or newer.

## The command-line tool

Pick whichever channel suits you. Every channel installs the same static binary.

### Go

```bash
go install github.com/tamnd/kv/cmd/kv@latest
```

### Homebrew (macOS and Linux)

```bash
brew install tamnd/tap/kv
```

### Scoop (Windows)

```bash
scoop bucket add tamnd https://github.com/tamnd/scoop-bucket
scoop install kv
```

### Linux (apt and dnf)

A signed apt and dnf repository tracks every release, so `apt upgrade` and `dnf upgrade` keep kv current.

```bash
# Debian, Ubuntu
curl -fsSL https://tamnd.github.io/linux-repo/gpg.key \
  | sudo gpg --dearmor -o /usr/share/keyrings/tamnd.gpg
echo "deb [signed-by=/usr/share/keyrings/tamnd.gpg] https://tamnd.github.io/linux-repo/apt stable main" \
  | sudo tee /etc/apt/sources.list.d/tamnd.list
sudo apt update && sudo apt install kv

# Fedora, RHEL
sudo dnf config-manager --add-repo https://tamnd.github.io/linux-repo/dnf/tamnd.repo
sudo dnf install kv
```

### Release archives and Linux packages

Every [release](https://github.com/tamnd/kv/releases) attaches `tar.gz` archives (and a `.zip` for Windows) for Linux, macOS, Windows, and FreeBSD, plus `.deb`, `.rpm`, and `.apk` packages and a `checksums.txt` with a cosign signature. Download the one for your platform, extract `kv`, and put it on your `PATH`. To install a package directly without the repository above:

```bash
# Debian/Ubuntu
sudo dpkg -i kv_*_amd64.deb

# Fedora/RHEL
sudo rpm -i kv-*.x86_64.rpm
```

### Container

The image is a minimal base plus the static binary:

```bash
docker run -v "$PWD/data:/data" ghcr.io/tamnd/kv create /data/app.kv
docker run -p 8480:8480 -v "$PWD/data:/data" ghcr.io/tamnd/kv \
  serve /data/app.kv --addr :8480 --insecure
```

Databases land under the mounted `/data` volume on your host.

## Verify the install

```bash
kv version
```

```
kv 0.2.0
  library  0.2.0
  commit   1a2b3c4
  built    2026-06-25T09:00:00Z
  go       go1.23.0 linux/amd64
```

The `library` line is the one that matters for data: two binaries reporting the same library version read and write the same on-disk format.

## Verify a download

Each release signs its `checksums.txt` with keyless [cosign](https://docs.sigstore.dev/). To verify an archive you downloaded:

```bash
cosign verify-blob \
  --certificate checksums.txt.pem \
  --signature checksums.txt.sig \
  --certificate-identity-regexp 'https://github.com/tamnd/kv' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt

sha256sum -c checksums.txt --ignore-missing
```

Next: [the quick start](/getting-started/quick-start/).
