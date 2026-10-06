use anyhow::{Ok, Result};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    server::WebPkiClientVerifier,
};
use std::{sync::Arc, vec};

pub const ALPN: &[u8] = b"rpc/1";

pub struct DevCA {
    cert: Certificate,
    key: KeyPair,
}

pub struct Identity {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl DevCA {
    pub fn new() -> Result<Self> {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(Vec::<String>::new())?;
        p.distinguished_name.push(DnType::CommonName, "dev-ca");
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = p.self_signed(&key)?;
        Ok(Self { cert, key })
    }
    pub fn issue(&self, name: &str) -> Result<Identity> {
        let key = KeyPair::generate()?;
        let mut p = CertificateParams::new(vec![name.to_string()])?;
        p.distinguished_name.push(DnType::CommonName, name);
        p.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = p.signed_by(&key, &self.cert, &self.key)?;
        Ok(Identity {
            chain: vec![cert.der().clone()],
            key: PrivatePkcs8KeyDer::from(key.serialize_der()).into(),
        })
    }
    pub fn roots(&self) -> Result<Arc<RootCertStore>> {
        let mut roots = RootCertStore::empty();
        roots.add(self.cert.der().clone());
        Ok(Arc::new(roots))
    }
    pub fn server_config(&self, id: Identity) -> Result<quinn::ServerConfig> {
        let verifier = WebPkiClientVerifier::builder(self.roots()?).build()?;
        let mut cfg = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(id.chain, id.key)?;
        cfg.alpn_protocols = vec![ALPN.to_vec()];
        Ok(quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(cfg)?,
        )))
    }
    pub fn client_config(&self, id: Identity) -> Result<quinn::ClientConfig> {
        let mut cfg = ClientConfig::builder()
            .with_root_certificates(self.roots()?)
            .with_client_auth_cert(id.chain, id.key)?;
        cfg.alpn_protocols = vec![ALPN.to_vec()];
        Ok(quinn::ClientConfig::new(Arc::new(
            QuicClientConfig::try_from(cfg)?,
        )))
    }
}
