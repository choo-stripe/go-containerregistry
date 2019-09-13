package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

type inMemoryCache struct {
	store map[string]*inMemLayer
}

// NewInMemoryCache returns a Cache implementation that stores data in-memory
func NewInMemoryCache() Cache {
	return &inMemoryCache{
		store: map[string]*inMemLayer{},
	}
}

func (ic *inMemoryCache) Put(l v1.Layer) (v1.Layer, error) {
	diffID, err := l.DiffID()
	if err != nil {
		return nil, err
	}

	// Digest() may or may not compress the layer.
	// If it does need to compress the layer, it throws out the compressed contents.
	// If someone's caching a layer, they're probably willing to pay the compression costs once.
	// Let's just take the overhead here, even though its a bit unpredictable, so we don't end
	// up compressing twice.
	compressedReadCloser, err := l.Compressed()
	if err != nil {
		return nil, err
	}
	defer compressedReadCloser.Close()

	hasher := sha256.New()
	combined := io.TeeReader(compressedReadCloser, hasher)

	contents, err := ioutil.ReadAll(combined)
	if err != nil {
		return nil, err
	}

	h := v1.Hash{
		Algorithm: "sha256",
		Hex:       hex.EncodeToString(hasher.Sum(make([]byte, 0, hasher.Size()))),
	}

	cachedLayer := &inMemLayer{
		Layer:      l,
		digest:     h,
		diffID:     diffID,
		compressed: contents,
	}
	ic.store[h.String()] = cachedLayer
	ic.store[diffID.String()] = cachedLayer

	return cachedLayer, nil
}

type inMemLayer struct {
	v1.Layer
	digest, diffID v1.Hash
	compressed     []byte
}

func (l *inMemLayer) Compressed() (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewBuffer(l.compressed)), nil
}

func (l *inMemLayer) Uncompressed() (io.ReadCloser, error) {
	return nil, errors.New("Uncompressed not implemented for in-memory cache")
}

func (ic *inMemoryCache) Get(h v1.Hash) (v1.Layer, error) {
	l, ok := ic.store[h.String()]
	if !ok {
		return nil, ErrNotFound
	}
	return l, nil
}

func (ic *inMemoryCache) Delete(h v1.Hash) error {
	_, ok := ic.store[h.String()]
	if !ok {
		return ErrNotFound
	}
	delete(ic.store, h.String())
	return nil
}
