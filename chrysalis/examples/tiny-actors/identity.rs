// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

use chrysalis::QuicIdentity;
use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::CertifiedIssuer;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::KeyPair;
use rcgen::KeyUsagePurpose;

pub fn mutually_trusted_identities(count: usize) -> Vec<QuicIdentity> {
    let mut issuer_params = CertificateParams::default();
    issuer_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    issuer_params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let issuer = CertifiedIssuer::self_signed(
        issuer_params,
        KeyPair::generate().expect("generate test issuer key"),
    )
    .expect("generate test issuer");
    let trust_roots = issuer.pem();
    (0..count)
        .map(|_| {
            let key = KeyPair::generate().expect("generate test key");
            let mut params = CertificateParams::new(vec!["localhost".to_owned()])
                .expect("construct test certificate parameters");
            params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
            params.extended_key_usages = vec![
                ExtendedKeyUsagePurpose::ClientAuth,
                ExtendedKeyUsagePurpose::ServerAuth,
            ];
            let certificate = params
                .signed_by(&key, &issuer)
                .expect("sign test certificate");
            QuicIdentity::new(
                certificate.der().as_ref(),
                format!("{}{}", certificate.pem(), trust_roots).into_bytes(),
                key.serialize_pem().into_bytes(),
                trust_roots.as_bytes().to_vec(),
                "localhost",
            )
        })
        .collect()
}
