package provider

import (
	"context"
	"fmt"

	"github.com/go-spatial/geom"
	"github.com/go-spatial/geom/slippy"
	"github.com/go-spatial/tegola/dict"
	"github.com/go-spatial/tegola/internal/log"
)

type providerType uint8

const (
	TypeStd providerType = iota
	TypeMvt
)

func (pt providerType) Prefix() string {
	if pt == TypeMvt {
		return "mvt_"
	}
	return ""
}

func (pt providerType) String() string {
	if pt == TypeMvt {
		return "MVT Provider"
	}
	return "Standard Provider"
}

type providerFilter uint8

const (
	ProviderFilterNone providerFilter = 0
	ProviderFilterStd                 = 1 << iota
	ProviderFilterMVT

	ProviderFilterAll = ProviderFilterStd | ProviderFilterMVT
)

// TODO(@ear7h) remove this atrocity from the code base
// tile_t is an implementation of the Tile interface, it is
// named as such as to not confuse from the 4 other possible meanings
// of the symbol "tile" in this code base. It should be removed after
// the geom port is mostly done as part of issue #499 (removing the
// Tile interface in this package)
type tile_t struct {
	slippy.Tile
	buffer uint
}

func NewTile(z, x, y, buf, srid uint) Tile {
	return &tile_t{
		Tile: slippy.Tile{
			Z: z,
			X: x,
			Y: y,
		},
		buffer: buf,
	}
}

func (tile *tile_t) Extent() (ext *geom.Extent, srid uint64) {
	return tile.Extent3857(), 3857
}

func (tile *tile_t) BufferedExtent() (ext *geom.Extent, srid uint64) {
	return tile.Extent3857().ExpandBy(slippy.Pixels2Webs(tile.Z, tile.buffer)), 3857
}

// Tile is an interface used by Tiler, it is an unnecessary abstraction and is
// due to be removed. The tiler interface will, instead take a, *geom.Extent.
type Tile interface {
	// ZXY returns the z, x and y values of the tile
	ZXY() (uint, uint, uint)
	// Extent returns the extent of the tile excluding any buffer
	Extent() (extent *geom.Extent, srid uint64)
	// BufferedExtent returns the extent of the tile including any buffer
	BufferedExtent() (extent *geom.Extent, srid uint64)
}

type Tiler interface {
	Layerer

	// TileFeature will stream decoded features to the callback function fn
	// if fn returns ErrCanceled, the TileFeatures method should stop processing
	TileFeatures(ctx context.Context, layer string, t Tile, fn func(f *Feature) error) error
}

// TilerUnion represents either a Std Tiler or and MVTTiler; only one should be not nil.
type TilerUnion struct {
	Std Tiler
	Mvt MVTTiler
}

func (tu TilerUnion) Layers() ([]LayerInfo, error) {
	if tu.Std != nil {
		return tu.Std.Layers()
	}
	if tu.Mvt != nil {
		return tu.Mvt.Layers()
	}
	return nil, ErrNilInitFunc
}

// InitFunc initialize a provider given a config map. The init function should validate the config map, and report any errors. This is called by the For function.
type InitFunc func(dicter dict.Dicter) (Tiler, error)

// CleanupFunc is called to when the system is shuting down, this allows the provider to cleanup.
type CleanupFunc func()

type pfns struct {
	// init will be filled out if it's a standard provider
	init InitFunc
	// mvtInit will be filled out if it's a mvt provider
	mvtInit MVTInitFunc

	cleanup CleanupFunc
}

var providers map[string]pfns

// Register the provider with the system. This call is generally made in the init functions of the provider.
// 	the clean up function will be called during shutdown of the provider to allow the provider to do any cleanup.
// The init function can not be nil, the cleanup function may be nil
func Register(name string, init InitFunc, cleanup CleanupFunc) error {
	if init == nil {
		return ErrNilInitFunc
	}
	if providers == nil {
		providers = make(map[string]pfns)
	}

	if _, ok := providers[name]; ok {
		return fmt.Errorf("provider %v already exists", name)
	}

	providers[name] = pfns{
		init:    init,
		cleanup: cleanup,
	}

	return nil
}

// MVTRegister the provider with the system. This call is generally made in the init functions of the provider.
// 	the clean up function will be called during shutdown of the provider to allow the provider to do any cleanup.
// The init function can not be nil, the cleanup function may be nil
func MVTRegister(name string, init MVTInitFunc, cleanup CleanupFunc) error {
	if init == nil {
		return ErrNilInitFunc
	}
	if providers == nil {
		providers = make(map[string]pfns)
	}

	if _, ok := providers[name]; ok {
		return fmt.Errorf("provider %v already exists", name)
	}

	providers[name] = pfns{
		mvtInit: init,
		cleanup: cleanup,
	}

	return nil
}

// Drivers returns a list of registered drivers.
func Drivers(FilterType providerFilter) (l []string) {
	if providers == nil || FilterType == ProviderFilterNone {
		return l
	}

	for k, v := range providers {
		switch FilterType {
		case ProviderFilterAll:
		case ProviderFilterMVT:
			if v.mvtInit == nil { // not of type mvt
				continue
			}
		case ProviderFilterStd:
			if v.init == nil { //not of type std
				continue
			}
		default:
			continue
		}
		l = append(l, k)
	}

	return l
}

// For function returns a configure provider of the given type; The provider may be a mvt provider or
// a std provider. The correct entry in TilerUnion will not be nil. If there is an error both entries
// will be nil.
func For(name string, config dict.Dicter) (val TilerUnion, err error) {
	var (
		driversList = Drivers(ProviderFilterAll)
	)
	if providers == nil {
		return val, ErrUnknownProvider{KnownProviders: driversList}
	}
	p, ok := providers[name]
	if !ok {
		return val, ErrUnknownProvider{KnownProviders: driversList, Name: name}
	}
	if p.init != nil {
		val.Std, err = p.init(config)
		return val, err
	}
	if p.mvtInit != nil {
		val.Mvt, err = p.mvtInit(config)
		return val, err
	}
	return val, ErrInvalidRegisteredProvider{Name: name}
}

// STDFor function returns a configured provider of the given type, provided the correct config map.
func STDFor(name string, config dict.Dicter) (Tiler, error) {
	err := ErrUnknownProvider{KnownProviders: Drivers(ProviderFilterStd)}
	if providers == nil {
		return nil, err
	}

	p, ok := providers[name]
	if !ok {
		err.Name = name
		return nil, err
	}
	// need to check to see if p if of type mvt provider
	if p.init == nil {
		return nil, ErrInvalidProviderType{
			Name:           name,
			Type:           TypeStd,
			KnownProviders: Drivers(ProviderFilterStd),
		}
	}

	return p.init(config)
}

// MVTFor function returns a configured mvt provider of the given type, provided the correct config map.
func MVTFor(name string, config dict.Dicter) (MVTTiler, error) {
	if providers == nil {
		return nil, ErrUnknownProvider{}
	}

	p, ok := providers[name]
	if !ok {
		return nil, ErrUnknownProvider{Name: name, KnownProviders: Drivers(ProviderFilterMVT)}
	}
	// need to check to see if p if of type mvt provider
	if p.mvtInit == nil {
		return nil, ErrInvalidProviderType{
			Name:           name,
			Type:           TypeMvt,
			KnownProviders: Drivers(ProviderFilterMVT),
		}
	}

	return p.mvtInit(config)
}

func Cleanup() {
	log.Info("cleaning up providers")
	for _, p := range providers {
		if p.cleanup != nil {
			p.cleanup()
		}
	}
}
