package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

const adapterCredentialEnvelopeV1 = "v1:"

// SecretBox seals small secret payloads for at-rest storage. aad should be
// stable row metadata, such as the credential key, so ciphertext cannot be
// moved between keys without failing authentication.
type SecretBox interface {
	Seal(plaintext, aad []byte) (string, error)
	Open(envelope string, aad []byte) ([]byte, error)
}

// AESGCMSecretBox is the stdlib-only SecretBox used by adapter credentials.
type AESGCMSecretBox struct {
	key [32]byte
}

// NewAESGCMSecretBox returns an AES-256-GCM box. The caller owns key
// derivation and must pass exactly 32 bytes.
func NewAESGCMSecretBox(key []byte) (*AESGCMSecretBox, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("store: adapter credential key must be 32 bytes, got %d", len(key))
	}
	var fixed [32]byte
	copy(fixed[:], key)
	return &AESGCMSecretBox{key: fixed}, nil
}

func (b *AESGCMSecretBox) Seal(plaintext, aad []byte) (string, error) {
	gcm, err := b.gcm()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("store: adapter credential nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, aad)
	payload := make([]byte, 0, len(nonce)+len(ciphertext))
	payload = append(payload, nonce...)
	payload = append(payload, ciphertext...)
	return adapterCredentialEnvelopeV1 + base64.StdEncoding.EncodeToString(payload), nil
}

func (b *AESGCMSecretBox) Open(envelope string, aad []byte) ([]byte, error) {
	if !strings.HasPrefix(envelope, adapterCredentialEnvelopeV1) {
		return nil, errors.New("store: adapter credential envelope version unsupported")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(envelope, adapterCredentialEnvelopeV1))
	if err != nil {
		return nil, fmt.Errorf("store: adapter credential envelope decode: %w", err)
	}
	gcm, err := b.gcm()
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(raw) < nonceSize {
		return nil, errors.New("store: adapter credential envelope truncated")
	}
	nonce := raw[:nonceSize]
	ciphertext := raw[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("store: adapter credential decrypt: %w", err)
	}
	return plaintext, nil
}

func (b *AESGCMSecretBox) gcm() (cipher.AEAD, error) {
	block, err := aes.NewCipher(b.key[:])
	if err != nil {
		return nil, fmt.Errorf("store: adapter credential cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("store: adapter credential gcm: %w", err)
	}
	return gcm, nil
}
