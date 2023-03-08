// Package compress provides wrappers for Fs and Object which implement compression.
package compress

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/chunkedreader"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fspath"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/operations"
)

// Register with Fs
func init() {
	// Register our remote
	fs.Register(&fs.RegInfo{
		Name:        "hard",
		Description: "Forcibly fixup read problems on remote",
		NewFs:       NewFs,
		MetadataInfo: &fs.MetadataInfo{
			Help: `Any metadata supported by the underlying remote is read and written.`,
		},
		Options: []fs.Option{{
			Name:     "remote",
			Help:     "Remote to work with.",
			Required: true,
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	Remote string `config:"remote"`
}

/*** FILESYSTEM FUNCTIONS ***/

// Fs represents a wrapped fs.Fs
type Fs struct {
	fs.Fs
	wrapper  fs.Fs
	name     string
	root     string
	opt      Options
	features *fs.Features // optional features
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, rpath string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	remote := opt.Remote
	if strings.HasPrefix(remote, name+":") {
		return nil, errors.New("can't point press remote at itself - check the value of the remote setting")
	}

	wInfo, wName, wPath, wConfig, err := fs.ConfigFs(remote)
	if err != nil {
		return nil, fmt.Errorf("failed to parse remote %q to wrap: %w", remote, err)
	}

	// Strip trailing slashes if they exist in rpath
	rpath = strings.TrimRight(rpath, "\\/")

	// First, check for a file
	// If a metadata file was found, return an error. Otherwise, check for a directory
	remotePath := wPath
	wrappedFs, err := wInfo.NewFs(ctx, wName, remotePath, wConfig)
	if err != fs.ErrorIsFile {
		remotePath = fspath.JoinRootPath(wPath, rpath)
		wrappedFs, err = wInfo.NewFs(ctx, wName, remotePath, wConfig)
	}
	if err != nil && err != fs.ErrorIsFile {
		return nil, fmt.Errorf("failed to make remote %s:%q to wrap: %w", wName, remotePath, err)
	}

	// Create the wrapping fs
	f := &Fs{
		Fs:   wrappedFs,
		name: name,
		root: rpath,
		opt:  *opt,
	}
	// the features here are ones we could support, and they are
	// ANDed with the ones from wrappedFs
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		DuplicateFiles:          false,
		ReadMimeType:            false,
		WriteMimeType:           false,
		GetTier:                 true,
		SetTier:                 true,
		BucketBased:             true,
		CanHaveEmptyDirectories: true,
		ReadMetadata:            true,
		WriteMetadata:           true,
		UserMetadata:            true,
	}).Fill(ctx, f).Mask(ctx, wrappedFs).WrapsFs(f, wrappedFs)
	// We can only support putstream if we have serverside copy or move
	if !operations.CanServerSideMove(wrappedFs) {
		f.features.Disable("PutStream")
	}

	return f, err
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
// List entries and process them
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	ret, err := f.Fs.List(ctx, dir)
	if err != nil {
		return entries, err
	}
	for _, x := range ret {
		if o, ok := x.(fs.Object); ok {
			entries = append(entries, &Object{
				Object: o,
				f:      f,
			})
		} else {
			entries = append(entries, x)
		}
	}
	return entries, err
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// Read metadata from metadata object
	o, err := f.Fs.NewObject(ctx, remote)
	if err != nil {
		return nil, err
	}
	return &Object{
		Object: o,
		f:      f,
	}, nil
}

// Put in to the remote path with the modTime given of the given size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	// If there's already an existent objects we need to make sure to explicitly update it to make sure we don't leave
	// orphaned data. Alternatively we could also deleted (which would simpler) but has the disadvantage that it
	// destroys all server-side versioning.
	return f.Fs.Put(ctx, in, src, options...)
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	do := f.Fs.Features().PutStream
	if do == nil {
		return nil, fs.ErrorCantCopy
	}
	return do(ctx, in, src, options...)
}

// Temporarily disabled. There might be a way to implement this correctly but with the current handling metadata duplicate objects
// will break stuff. Right no I can't think of a way to make this work.

// PutUnchecked uploads the object
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set {
	return f.Fs.Hashes()
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return f.Fs.Mkdir(ctx, dir)
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return f.Fs.Rmdir(ctx, dir)
}

// Purge all files in the root and the root directory
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	do := f.Fs.Features().Purge
	if do == nil {
		return fs.ErrorCantPurge
	}
	return do(ctx, dir)
}

// Copy src to this remote using server side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	do := f.Fs.Features().Copy
	if do == nil {
		return nil, fs.ErrorCantCopy
	}
	o, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantCopy
	}

	return do(ctx, o.Object, remote)
}

// Move src to this remote using server side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	do := f.Fs.Features().Move
	if do == nil {
		return nil, fs.ErrorCantMove
	}
	o, ok := src.(*Object)
	if !ok {
		return nil, fs.ErrorCantMove
	}

	return do(ctx, o.Object, remote)
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove
//
// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	do := f.Fs.Features().DirMove
	if do == nil {
		return fs.ErrorCantDirMove
	}
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	return do(ctx, srcFs.Fs, srcRemote, dstRemote)
}

// CleanUp the trash in the Fs
//
// Implement this if you have a way of emptying the trash or
// otherwise cleaning up old versions of files.
func (f *Fs) CleanUp(ctx context.Context) error {
	do := f.Fs.Features().CleanUp
	if do == nil {
		return errors.New("not supported by underlying remote")
	}
	return do(ctx)
}

// About gets quota information from the Fs
func (f *Fs) About(ctx context.Context) (*fs.Usage, error) {
	do := f.Fs.Features().About
	if do == nil {
		return nil, errors.New("not supported by underlying remote")
	}
	return do(ctx)
}

// UnWrap returns the Fs that this Fs is wrapping
func (f *Fs) UnWrap() fs.Fs {
	return f.Fs
}

// WrapFs returns the Fs that is wrapping this Fs
func (f *Fs) WrapFs() fs.Fs {
	return f.wrapper
}

// SetWrapper sets the Fs that is wrapping this Fs
func (f *Fs) SetWrapper(wrapper fs.Fs) {
	f.wrapper = wrapper
}

// MergeDirs merges the contents of all the directories passed
// in into the first one and rmdirs the other directories.
func (f *Fs) MergeDirs(ctx context.Context, dirs []fs.Directory) error {
	do := f.Fs.Features().MergeDirs
	if do == nil {
		return errors.New("MergeDirs not supported")
	}
	out := make([]fs.Directory, len(dirs))
	for i, dir := range dirs {
		out[i] = fs.NewDirCopy(ctx, dir).SetRemote(dir.Remote())
	}
	return do(ctx, out)
}

// DirCacheFlush resets the directory cache - used in testing
// as an optional interface
func (f *Fs) DirCacheFlush() {
	do := f.Fs.Features().DirCacheFlush
	if do != nil {
		do()
	}
}

// ChangeNotify calls the passed function with a path
// that has had changes. If the implementation
// uses polling, it should adhere to the given interval.
func (f *Fs) ChangeNotify(ctx context.Context, notifyFunc func(string, fs.EntryType), pollIntervalChan <-chan time.Duration) {
	do := f.Fs.Features().ChangeNotify
	if do == nil {
		return
	}
	do(ctx, notifyFunc, pollIntervalChan)
}

// PublicLink generates a public link to the remote path (usually readable by anyone)
func (f *Fs) PublicLink(ctx context.Context, remote string, duration fs.Duration, unlink bool) (string, error) {
	do := f.Fs.Features().PublicLink
	if do == nil {
		return "", errors.New("can't PublicLink: not supported by underlying remote")
	}
	o, err := f.NewObject(ctx, remote)
	if err != nil {
		// assume it is a directory
		return do(ctx, remote, duration, unlink)
	}
	return do(ctx, o.(*Object).Object.Remote(), duration, unlink)
}

/*** OBJECT FUNCTIONS ***/

// Object with external metadata
type Object struct {
	fs.Object     // Wraps around data object for this object
	f         *Fs // Filesystem object is in
}

// Remove removes this object
func (o *Object) Remove(ctx context.Context) error {
	return o.Object.Remove(ctx)
}

// ReadCloserWrapper combines a Reader and a Closer to a ReadCloser
type ReadCloserWrapper struct {
	io.Reader
	io.Closer
}

// Update in to the object with the modTime given of the given size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	return o.Object.Update(ctx, in, src, options...)
}

// Shutdown the backend, closing any background tasks and any
// cached connections.
func (f *Fs) Shutdown(ctx context.Context) error {
	do := f.Fs.Features().Shutdown
	if do == nil {
		return nil
	}
	return do(ctx)
}

// Fs returns read only access to the Fs that this object is part of
func (o *Object) Fs() fs.Info {
	return o.f
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Hard: %s", o.Remote())
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.Object.Remote()
}

// Size returns the size of the file
func (o *Object) Size() int64 {
	return o.Object.Size()
}

// MimeType returns the MIME type of the file
func (o *Object) MimeType(ctx context.Context) string {
	mt, ok := o.Object.(fs.MimeTyper)
	if !ok {
		return ""
	}
	return mt.MimeType(ctx)
}

// Metadata returns metadata for an object
//
// It should return nil if there is no Metadata
func (o *Object) Metadata(ctx context.Context) (fs.Metadata, error) {
	do, ok := o.Object.(fs.Metadataer)
	if !ok {
		return nil, nil
	}
	return do.Metadata(ctx)
}

// Hash returns the selected checksum of the file
// If no checksum is available it returns ""
func (o *Object) Hash(ctx context.Context, ht hash.Type) (string, error) {
	return o.Object.Hash(ctx, ht)
}

// SetTier performs changing storage tier of the Object if
// multiple storage classes supported
func (o *Object) SetTier(tier string) error {
	do, ok := o.Object.(fs.SetTierer)
	if !ok {
		return errors.New("object cant set tier")
	}
	return do.SetTier(tier)
}

// GetTier returns storage tier or class of the Object
func (o *Object) GetTier() string {
	do, ok := o.Object.(fs.GetTierer)
	if !ok {
		return ""
	}
	return do.GetTier()
}

// UnWrap returns the wrapped Object
func (o *Object) UnWrap() fs.Object {
	return o.Object
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser. Note that this call requires quite a bit of overhead.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (rc io.ReadCloser, err error) {
	// Get offset and limit from OpenOptions, pass the rest to the underlying remote
	var openOptions = []fs.OpenOption{}
	var offset, limit int64 = 0, -1
	for _, option := range options {
		switch x := option.(type) {
		case *fs.SeekOption:
			offset = x.Offset
		case *fs.RangeOption:
			offset, limit = x.Decode(o.Size())
		default:
			openOptions = append(openOptions, option)
		}
	}

	return &hardReader{
		o:       o.Object,
		offset:  offset,
		limit:   limit,
		options: openOptions,
	}, nil
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	do, ok := o.Object.(fs.IDer)
	if !ok {
		return ""
	}
	return do.ID()
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Return a string version
func (f *Fs) String() string {
	return fmt.Sprintf("Hard: %s:%s", f.name, f.root)
}

type hardReader struct {
	o          fs.Object
	rc         io.ReadCloser
	options    []fs.OpenOption
	offset     int64
	limit      int64
	eofReached bool
	closed     bool
}

func appendSeekOption(options []fs.OpenOption, offset, limit int64) []fs.OpenOption {
	if limit == -1 {
		if offset > 0 {
			// start from offset, read until end
			return append(options, &fs.SeekOption{Offset: offset})
		}
		// else: start from 0, read until end
	} else {
		if offset > 0 {
			// start from offset, with limit
			return append(options, &fs.RangeOption{Start: offset, End: limit})
		}
	}
	return options
}

func (r *hardReader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, chunkedreader.ErrorFileClosed
	}
	if r.eofReached {
		return 0, io.EOF
	}
	if r.limit != -1 && r.limit <= r.offset {
		r.eofReached = true
		return 0, io.EOF
	}
	defer func() {
		fs.Debugf(r.o, "result: %d %d %d %v", r.offset, r.limit, n, err)
	}()
	for {
		if r.rc == nil {
			newOpts := []fs.OpenOption{}
			newOpts = append(newOpts, r.options...)
			newOpts = appendSeekOption(newOpts, r.offset, r.limit)
			r.rc, err = r.o.Open(context.Background(), newOpts...)
			if err != nil {
				fs.Errorf(r.o, "err on open: %v", err)
				r.rc = nil
				continue
			}
		}
		n, err = r.rc.Read(p)
		if err == io.EOF {
			// EOF
			r.eofReached = true
			return n, err
		}
		if err != nil {
			fs.Errorf(r.o, "err on read: %v", err)
			r.rc = nil
			if n > 0 {
				r.offset += int64(n)
				return n, nil
			}
			continue
		}
		r.offset += int64(n)
		return n, err
	}
}
func (r *hardReader) Close() (err error) {
	if r.closed {
		return chunkedreader.ErrorFileClosed
	}
	if r.rc != nil {
		err = r.rc.Close()
	}
	r.rc = nil
	r.o = nil
	r.closed = true
	return err
}

// Check the interfaces are satisfied
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Purger          = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.PutStreamer     = (*Fs)(nil)
	_ fs.CleanUpper      = (*Fs)(nil)
	_ fs.UnWrapper       = (*Fs)(nil)
	_ fs.Abouter         = (*Fs)(nil)
	_ fs.Wrapper         = (*Fs)(nil)
	_ fs.MergeDirser     = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.ChangeNotifier  = (*Fs)(nil)
	_ fs.PublicLinker    = (*Fs)(nil)
	_ fs.Shutdowner      = (*Fs)(nil)
	_ fs.FullObject      = (*Object)(nil)
	_ io.ReadCloser      = (*hardReader)(nil)
	_ io.Reader          = (*hardReader)(nil)
	_ io.Closer          = (*hardReader)(nil)
)
