/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use chrysalis::Pid;
use chrysalis::QuicIdentity;
use chrysalis::certificate_pid;
use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::CertifiedIssuer;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::KeyPair;
use rcgen::KeyUsagePurpose;

/// PEM text and certificate-derived PID generated together for one role.
pub(crate) struct IdentityBundle {
    /// Leaf certificate, trusted CA certificate, and private key as PEM.
    pub(crate) pem: String,
    /// PID derived directly from the leaf certificate bytes.
    pub(crate) pid: Pid,
}

pub(crate) fn mutually_trusted_bundles<const N: usize>() -> [IdentityBundle; N] {
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
    std::array::from_fn(|_| {
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
        IdentityBundle {
            pid: certificate_pid(certificate.der().as_ref()),
            pem: format!(
                "{}\n{}\n{}",
                certificate.pem().trim_end(),
                trust_roots.trim_end(),
                key.serialize_pem().trim_end(),
            ),
        }
    })
}

pub(crate) fn mutually_trusted_identities<const N: usize>() -> [QuicIdentity; N] {
    mutually_trusted_bundles::<N>().map(|bundle| {
        let identity = QuicIdentity::from_pem_bundle(&bundle.pem, "localhost")
            .expect("load generated identity bundle");
        assert_eq!(identity.pid(), bundle.pid);
        identity
    })
}
