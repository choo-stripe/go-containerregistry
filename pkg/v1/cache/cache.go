// Package cache provides methods to cache layers.
package cache

import (
	"errors"
	"io/ioutil"
	"sync"

	"github.com/google/go-containerregistry/pkg/logs"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"golang.org/x/sync/errgroup"
)

// Cache encapsulates methods to interact with cached layers.
type Cache interface {
	// Put writes the Layer to the Cache.
	//
	// The returned Layer should be used for future operations, since lazy
	// cachers might only populate the cache when the layer is actually
	// consumed.
	//
	// The returned layer can be consumed, and the cache entry populated,
	// by calling either Compressed or Uncompressed and consuming the
	// returned io.ReadCloser.
	Put(v1.Layer) (v1.Layer, error)

	// Get returns the Layer cached by the given Hash, or ErrNotFound if no
	// such layer was found.
	Get(v1.Hash) (v1.Layer, error)

	// Delete removes the Layer with the given Hash from the Cache.
	Delete(v1.Hash) error
}

// ErrNotFound is returned by Get when no layer with the given Hash is found.
var ErrNotFound = errors.New("layer was not found")

// Image returns a new Image which wraps the given Image, whose layers will be
// pulled from the Cache if they are found, and written to the Cache as they
// are read from the underlying Image.
func Image(i v1.Image, c Cache) *CachedImage {
	return &CachedImage{
		Image: i,
		c:     c,
	}
}

type CachedImage struct {
	v1.Image
	c         Cache
	layerOnce sync.Once
	layers    []v1.Layer
}

func (i *CachedImage) Layers() ([]v1.Layer, error) {
	var outerErr error
	i.layerOnce.Do(func() {
		ls, err := i.Image.Layers()
		if err != nil {
			outerErr = err
			return
		}

		i.layers = make([]v1.Layer, len(ls), len(ls))
		var g errgroup.Group

		for idx, l := range ls {
			idx := idx
			l := l

			g.Go(func() error {
				// Check if this layer is present in the cache in compressed
				// form.
				digest, err := l.Digest()
				if err != nil {
					return err
				}
				if cl, err := i.c.Get(digest); err == nil {
					// Layer found in the cache.
					logs.Progress.Printf("Layer %s found (compressed) in cache", digest)
					i.layers[idx] = cl
					return nil
				} else if err != nil && err != ErrNotFound {
					return err
				}

				// Check if this layer is present in the cache in
				// uncompressed form.
				diffID, err := l.DiffID()
				if err != nil {
					return err
				}
				if cl, err := i.c.Get(diffID); err == nil {
					// Layer found in the cache.
					logs.Progress.Printf("Layer %s found (uncompressed) in cache", diffID)
					i.layers[idx] = cl
				} else if err != nil && err != ErrNotFound {
					return err
				}

				// Not cached, fall through to real layer.
				l, err = i.c.Put(l)
				if err != nil {
					return err
				}
				i.layers[idx] = l

				return nil
			})
		}

		err = g.Wait()
		if err != nil {
			outerErr = err
		}
	})

	if outerErr != nil {
		return nil, outerErr
	}

	return i.layers, nil
}

func (i *CachedImage) LayerByDigest(h v1.Hash) (v1.Layer, error) {
	l, err := i.c.Get(h)
	if err == ErrNotFound {
		// Not cached, get it and write it.
		l, err := i.Image.LayerByDigest(h)
		if err != nil {
			return nil, err
		}
		return i.c.Put(l)
	}
	return l, err
}

func (i *CachedImage) LayerByDiffID(h v1.Hash) (v1.Layer, error) {
	l, err := i.c.Get(h)
	if err == ErrNotFound {
		// Not cached, get it and write it.
		l, err := i.Image.LayerByDiffID(h)
		if err != nil {
			return nil, err
		}
		return i.c.Put(l)
	}
	return l, err
}

func (i *CachedImage) Populate() error {
	layers, err := i.Layers()
	if err != nil {
		return err
	}

	// read all layers in parallel
	var g errgroup.Group
	for _, l := range layers {
		l := l
		g.Go(func() error {
			// put the layer in the cache so were populating the cached layer
			l, err = i.c.Put(l)
			if err != nil {
				return err
			}

			compressedReader, err := l.Compressed()
			if err != nil {
				return err
			}
			defer compressedReader.Close()

			_, err = ioutil.ReadAll(compressedReader)
			if err != nil {
				return err
			}

			return nil
		})
	}

	return g.Wait()
}
