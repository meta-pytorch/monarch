/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

use chrysalis::QuicIdentity;
use rcgen::BasicConstraints;
use rcgen::CertificateParams;
use rcgen::CertifiedIssuer;
use rcgen::ExtendedKeyUsagePurpose;
use rcgen::IsCa;
use rcgen::KeyPair;
use rcgen::KeyUsagePurpose;

pub(crate) fn mutually_trusted_identities<const N: usize>() -> [QuicIdentity; N] {
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
        QuicIdentity::new(
            certificate.der().as_ref(),
            format!("{}\n{}", certificate.pem().trim_end(), trust_roots).into_bytes(),
            key.serialize_pem().into_bytes(),
            trust_roots.as_bytes().to_vec(),
            "localhost",
        )
    })
}
