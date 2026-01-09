#!/bin/bash
# Generate development TLS certificates for PrkDB Raft cluster
# Creates CA, server, and client certificates for secure gRPC

set -e

CERT_DIR="${1:-certs}"
DAYS=365

echo ""
echo "🔐 PrkDB TLS Certificate Generator"
echo "==================================="
echo ""

mkdir -p "$CERT_DIR"
cd "$CERT_DIR"

# Generate CA
echo "📜 Generating Certificate Authority..."
openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -new -x509 -days $DAYS -key ca.key -out ca.crt \
    -subj "/CN=PrkDB CA/O=PrkDB/C=US" 2>/dev/null
echo "   ✅ CA certificate: $CERT_DIR/ca.crt"

# Generate server key and certificate
echo "📜 Generating server certificate..."
openssl genrsa -out server.key 2048 2>/dev/null

# Create server CSR with SANs for localhost
cat > server.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = localhost
DNS.2 = *.local
IP.1 = 127.0.0.1
IP.2 = ::1
EOF

openssl req -new -key server.key -out server.csr \
    -subj "/CN=localhost/O=PrkDB/C=US" 2>/dev/null
openssl x509 -req -days $DAYS -in server.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out server.crt -extfile server.ext 2>/dev/null
echo "   ✅ Server certificate: $CERT_DIR/server.crt"

# Generate client key and certificate
echo "📜 Generating client certificate..."
openssl genrsa -out client.key 2048 2>/dev/null

cat > client.ext <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

openssl req -new -key client.key -out client.csr \
    -subj "/CN=prkdb-client/O=PrkDB/C=US" 2>/dev/null
openssl x509 -req -days $DAYS -in client.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out client.crt -extfile client.ext 2>/dev/null
echo "   ✅ Client certificate: $CERT_DIR/client.crt"

# Cleanup CSR and ext files
rm -f *.csr *.ext *.srl

echo ""
echo "📁 Certificates generated in: $CERT_DIR/"
echo ""
ls -la
echo ""
echo "🔒 Usage:"
echo "   Server: --tls-cert server.crt --tls-key server.key --tls-ca ca.crt"
echo "   Client: --tls-cert client.crt --tls-key client.key --tls-ca ca.crt"
echo ""
