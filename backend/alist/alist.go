// Package alist provides an interface to Alist's interface
// as a rclone remote
package alist

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/rest"
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "alist",
		Description: "Alist",
		NewFs:       NewFs,

		Options: []fs.Option{{
			Name: "url",
			Help: "The URL to an instance of Alist.",
		}, {
			// unused as it can't write
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default:  0,
		},
		}})
}

var (
	errorReadOnly = errors.New("alist remotes are read only")
	timeUnset     = time.Unix(0, 0)
)

// Options defines the configuration for this backend
type Options struct {
	Url string               `config:"url"`
	Enc encoder.MultiEncoder `config:"encoding"`
}

// Fs represents an IAS3 remote
type Fs struct {
	name      string       // name of this remote
	root      string       // the path we are working on if any
	inputRoot string       // the user-specified root
	opt       Options      // parsed config options
	features  *fs.Features // optional features
	srv       *rest.Client // the connection to the instance
	ctx       context.Context
}

// Object describes a file at Alist
type Object struct {
	fs         *Fs       // reference to Fs
	remote     string    // the remote path
	remotePath string    // the remote path on server
	modTime    time.Time // last modified time
	size       int64     // size of the file in bytes
}

// ListResponse contains the structure for /api/fs/list
type ListResponse struct {
	Code    int      `json:"code"`
	Data    ListData `json:"data"`
	Message string   `json:"message"`
}

// ListData contains the structure for .data in /api/fs/list
type ListData struct {
	Content []ListContent `json:"content"`
	// discard all other fields
}

// ListContent contains the structure for .data.content[] in /api/fs/list
type ListContent struct {
	IsDir    bool   `json:"is_dir"`
	Modified string `json:"modified"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	// discard all other fields
}

// GetResponse contains the structure for /api/fs/get
type GetResponse struct {
	Code    int     `json:"code"`
	Data    GetData `json:"data"`
	Message string  `json:"message"`
}

// GetData contains the structure for .data in /api/fs/get
type GetData struct {
	IsDir    bool   `json:"is_dir"`
	Modified string `json:"modified"`
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	RawUrl   string `json:"raw_url"`
	// discard all other fields
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	ep, err := url.Parse(f.opt.Url)
	instance, path := "!!UNKNOWN!!", ""
	if err == nil {
		instance = ep.Host
		path = ep.Path
	}

	if path == "" {
		return fmt.Sprintf("Alist instance (%s) root", instance)
	}
	return fmt.Sprintf("Alist instance (%s) at path %s", instance, path)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Hashes returns type of hashes supported by Alist: none.
func (f *Fs) Hashes() hash.Set {
	return hash.NewHashSet(hash.None)
}

// Precision returns the precision of mtime that the server responds
func (f *Fs) Precision() time.Duration {
	// the JSON contains: "2022-09-25T12:41:37Z"
	return time.Second // so seconds.
}

// NewFs constructs an Fs from the path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// Parse the endpoints
	ep, err := url.Parse(opt.Url)
	if err != nil {
		return nil, err
	}

	inputRoot := root
	root = strings.Trim(path.Join(ep.Path, root), "/")

	f := &Fs{
		name:      name,
		opt:       *opt,
		ctx:       ctx,
		inputRoot: inputRoot,
	}
	f.setRoot(root)
	f.features = (&fs.Features{}).Fill(ctx, f)

	f.srv = rest.NewClient(fshttp.NewClient(ctx))
	// let's build the user part
	ui, userInfo := ep.User, ""
	if ui != nil {
		userInfo = fmt.Sprintf("%s@", ui.String())
	}
	f.srv.SetRoot(fmt.Sprintf("%s://%s%s", ep.Scheme, userInfo, ep.Host))

	// test if the root exists as a file
	_, err = f.NewObject(ctx, "/")
	if err == nil {
		f.setRoot(betterPathDir(root))
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// setRoot changes the root of the Fs
func (f *Fs) setRoot(root string) {
	f.root = strings.Trim(root, "/")
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime is the last modified time (read-only)
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Size is the file length
func (o *Object) Size() int64 {
	return o.size
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Hash returns the hash value presented by IA
func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Storable returns if this object is storable
func (o *Object) Storable() bool {
	return true
}

// SetModTime sets modTime on a particular file
func (o *Object) SetModTime(ctx context.Context, t time.Time) (err error) {
	return errorReadOnly
}

// List files and directories in a directory
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	var bodyJson []byte
	remoteDir := path.Join("/", f.root, dir)
	// fmt.Printf("%s %s\n", dir, remoteDir)
	for page := 1; ; page++ {
		bodyJson, err = json.Marshal(map[string]interface{}{
			"page":     page,
			"password": "",
			"path":     remoteDir,
			"per_page": 30,
			"refresh":  false,
		})
		if err != nil {
			return
		}
		opts := rest.Opts{
			Method: "POST",
			Path:   "/api/fs/list",
			Body:   bytes.NewReader(bodyJson),
			ExtraHeaders: map[string]string{
				"content-type": "application/json;charset=UTF-8",
				"origin":       "https://files.nogizaka46.cc",
				"referer":      "https://files.nogizaka46.cc/",
			},
		}

		var temp ListResponse
		_, err = f.srv.CallJSON(ctx, &opts, nil, &temp)
		if err != nil {
			return
		}

		// assert if it success or not
		if temp.Code == 500 {
			// the exact message is "object not found"
			return nil, fs.ErrorObjectNotFound
		}
		if temp.Code != 200 {
			return entries, fmt.Errorf("the instance says: %s (code %d)", temp.Message, temp.Code)
		}

		for _, item := range temp.Data.Content {
			mtime, err := time.Parse(time.RFC3339, item.Modified)
			if err != nil {
				mtime = timeUnset
			}
			// fmt.Printf("%s %d %d\n", item.Name, item.IsDir, item.Size)
			rname := path.Join(dir, item.Name)
			if item.IsDir {
				entries = append(entries, fs.NewDir(rname, mtime))
			} else {
				entries = append(entries, &Object{
					fs:         f,
					remote:     rname,
					remotePath: path.Join(f.root, dir, item.Name),
					size:       item.Size,
					modTime:    mtime,
				})
			}
		}

		if len(temp.Data.Content) < 30 {
			break
		}
	}

	return entries, nil
}

// Mkdir can't be done
func (f *Fs) Mkdir(ctx context.Context, dir string) (err error) {
	return errorReadOnly
}

// Rmdir can't be done
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (ret fs.Object, err error) {
	remotePath := path.Join("/", f.root, remote)
	bodyJson, err := json.Marshal(map[string]interface{}{
		"password": "",
		"path":     remotePath,
	})
	if err != nil {
		return
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/fs/get",
		Body:   bytes.NewReader(bodyJson),
	}

	var temp GetResponse
	_, err = f.srv.CallJSON(ctx, &opts, nil, &temp)
	if err != nil {
		return
	}

	// assert if it success or not
	if temp.Code == 500 {
		// the exact message is "object not found"
		return nil, fs.ErrorObjectNotFound
	}
	if temp.Code != 200 {
		return nil, fmt.Errorf("the instance says: %s (code %d)", temp.Message, temp.Code)
	}

	// assert we're not getting a directory
	if temp.Data.IsDir {
		return nil, fs.ErrorIsDir
	}

	item := temp.Data
	mtime, err := time.Parse(time.RFC3339, item.Modified)
	if err != nil {
		mtime = timeUnset
	}
	return &Object{
		fs:         f,
		remote:     remote,
		remotePath: remotePath,
		size:       int64(item.Size),
		modTime:    mtime,
	}, nil
}

// Put uploads a file
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}

// PublicLink generates a public link to the remote path (usually readable by anyone)
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (link string, err error) {
	if strings.HasSuffix(remote, "/") {
		return "", fs.ErrorCantShareDirectories
	}
	// example link: https://alist-instance.localhost/d/link/to/file.txt
	remotePath := path.Join("/", "d", f.root, remote)

	ep, err := url.Parse(f.opt.Url)
	if err != nil {
		// unreachable: checked once at NewFs
		return "", err
	}

	return fmt.Sprintf("%s://%s%s", ep.Scheme, ep.Host, remotePath), nil
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (_ fs.Object, err error) {
	return nil, errorReadOnly
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	var optionsFixed []fs.OpenOption
	for _, opt := range options {
		if optRange, ok := opt.(*fs.RangeOption); ok {
			// Ignore range option if file is empty
			if o.Size() == 0 && optRange.Start == 0 && optRange.End > 0 {
				continue
			}
		}
		optionsFixed = append(optionsFixed, opt)
	}

	var resp *http.Response
	opts := rest.Opts{
		Method:  "GET",
		Path:    path.Join("/", o.remotePath),
		Options: optionsFixed,
	}
	resp, err = o.fs.srv.Call(ctx, &opts)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Update the Object from in with modTime and size
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	return errorReadOnly
}

// Remove an object
func (o *Object) Remove(ctx context.Context) (err error) {
	return errorReadOnly
}

// String converts this Fs to a string
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

func betterPathDir(p string) string {
	d := path.Dir(p)
	if d == "." {
		return ""
	}
	return d
}

var (
	_ fs.Fs     = &Fs{}
	_ fs.Object = &Object{}
)
