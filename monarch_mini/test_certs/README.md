# Test certificates

Generate the local TLS fixtures before running Rust or Python QUIC tests:

```sh
cd fbcode/monarch/monarch_mini
./test_certs/generate.sh
```

The generated `ca.pem`, `cert.pem`, and `key.pem` files are ignored by source
control. Run the script again whenever you need to replace them.
