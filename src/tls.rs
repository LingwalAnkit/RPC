use anyhow::{Ok, Result};
// anyhow::Result lets your functions return:
// instead of manually specifying every possible error type.
// Success → Self
// Failure → anyhow::Error
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
// Rustls handles TLS encryption/authentication.
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
// rcgen is being used to generate certificates and keys.
// You'll use it to create:
// 1. CA private key
// 2. CA certificate
// 3. Server private key + certificate
// 4. Client private key + certificate
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};
// CertificateDer Represents a certificate in DER format.
// PrivateKeyDer Represents a private key in DER format.
// PrivatePkcs8KeyDer Represents a private key in PKCS#8 format.
// WebPkiClientVerifier Used by the server to verify client certificates.
use std::{sync::Arc, vec};
// arc let multiple part of program share data safley

pub const ALPN: &[u8] = b"rpc/1";
// Refrence to byte slice &[u8] and b"rpc/1" -> store in byte format
// Application protocal layer negotiation it tells tls the application layer protocol we are talking over is rpc/1

pub struct DevCA {
    cert: Certificate, // ca certificate
    key: KeyPair,      // ca private key it allow ca to sign certificates
}
// dev certificate authority

pub struct Identity {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl DevCA {
    pub fn new() -> Result<Self> {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(Vec::<String>::new())?;
        //cretae rcgeb certificate params
        // Vec::<String>::new() creates a new empty vector
        p.distinguished_name.push(DnType::CommonName, "dev-ca");
        //giving name to certificate which is dev-ca
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        //mark the certificate as ca[certificate authority]
        p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        //allow the ca to sign certificates and revoke Certificate Revocation Lists.
        // crl is basically certificate that no longer be trusted
        let cert = p.self_signed(&key)?;
        // sign itself
        Ok(Self { cert, key })
    }
    pub fn issue(&self, name: &str) -> Result<Identity> {
        let key = KeyPair::generate()?;
        // this is server key or client key
        let mut p = CertificateParams::new(vec![name.to_string()])?;
        // can have multiple names that is why we are using vec here
        // creates certificate params where name is used as alternative name
        p.distinguished_name.push(DnType::CommonName, name);
        // common name
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        // the certificate can be used for server auth and client auth
        let cert = p.signed_by(&key, &self.cert, &self.key)?;
        // sign the certificate with ca
        Ok(Identity {
            chain: vec![cert.der().clone()], // certificate is converted to der format
            // .clone makes an own copy
            //
            key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(), // key is also serialized to der bytes into() convert converts the PrivatePkcs8KeyDer into the type required by Identity: -> privatekeyder
        })
        // der is the format in which certificate and keys are encoded -> bytes
    }
    pub fn roots(&self) -> Result<Arc<RootCertStore>> {
        // this create a trust store
        // arc let multiple part of program share ownership of same value safley
        let mut roots = RootCertStore::empty();
        roots.add(self.cert.der().clone());
        // add ca certificate here
        // this means trust certificate signed by ca
        Ok(Arc::new(roots))
    }
    pub fn server_config(&self, id: Identity) -> Result<quinn::ServerConfig> {
        // create a quic server configuration
        let verifier = WebPkiClientVerifier::builder(self.roots()?).build()?;
        // verify client certificate
        // I will only accept clients whose certificate chains back to my trusted CA.
        let mut cfg = ServerConfig::builder()
            .with_client_cert_verifier(verifier) // mutual tls
            .with_single_cert(id.chain, id.key)?; //giver server its cert and key
        // building tls server
        // create ruslts config
        // start creatinf server ServerConfig::builder
        cfg.alpn_protocols = vec![ALPN.to_vec()]; // u8 -> vec and cfg.alpn_protocol takes vec<vec<u8>>
        Ok(quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(cfg)?,
        )))
        // quic quinn server
        // "Convert this rustls ServerConfig into a Quinn-compatible QUIC TLS configuration." -> QuicServerConfig and it has its own QUIC-compatible crypto configuration:
    }
    pub fn client_config(&self, id: Identity) -> Result<quinn::ClientConfig> {
        let mut cfg = ClientConfig::builder()
            .with_root_certificates(self.roots()?) // i trust certificate sign bt devca
            // means client trusts DevCA → uses it to verify the server certificate.
            .with_client_auth_cert(id.chain, id.key)?; // give client its own cert and key
        cfg.alpn_protocols = vec![ALPN.to_vec()];
        Ok(quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(cfg)?,
        )))
    }
}

// 1. Client connects to server
//              ↓
// 2. TLS handshake starts
//              ↓
// 3. Server presents server certificate
//              ↓
// 4. Client checks:
// "Was this signed by DevCA?"
//              ↓
// 5. Client presents client certificate
//              ↓
// 6. Server checks:
// "Was this signed by DevCA?"
//              ↓
// 7. Both identities verified
//              ↓
// 8. ALPN negotiates rpc/1
//              ↓
// 9. QUIC connection established
//              ↓
// 10. Your RPC protocol runs
