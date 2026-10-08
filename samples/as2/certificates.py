"""Generate isolated demonstration identities, never production credentials."""
from datetime import datetime, timedelta, timezone
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def identity(name):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = datetime.now(timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now-timedelta(minutes=1)).not_valid_after(now+timedelta(days=7))
            .add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True)
            .sign(key,hashes.SHA256()))
    public = cert.public_bytes(serialization.Encoding.PEM).decode()
    private = key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption()).decode()
    return private+public, public
