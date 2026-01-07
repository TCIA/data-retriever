package app

import (
    "fmt"
    "os"
    "path/filepath"
    "time"

    "github.com/DavidGamba/go-getoptions"
)

var (
    TokenUrl = "https://nbia-stage.cancerimagingarchive.net/nbia-api/oauth/token"
    ImageUrl = "https://nbia-stage.cancerimagingarchive.net/nbia-api/services/v4/getImage"
    MetaUrl  = "https://nbia-stage.cancerimagingarchive.net/nbia-api/services/v4/getSeriesMetadata"
)

// Options captures command line parameters shared between CLI and GUI.
type Options struct {
    Input           string
    Output          string
    Proxy           string
    Concurrent      int
    Meta            bool
    Username        string
    Password        string
    Version         bool
    Debug           bool
    Help            bool
    MetaUrl         string
    TokenUrl        string
    ImageUrl        string
    SaveLog         bool
    Prompt          bool
    Force           bool
    SkipExisting    bool
    MaxRetries      int
    RetryDelay      time.Duration
    MaxConnsPerHost int
    ServerFriendly  bool
    RequestDelay    time.Duration
    NoMD5           bool
    NoDecompress    bool
    RefreshMetadata bool
    MetadataWorkers int

    opt *getoptions.GetOpt
}

// InitOptions parses command-line arguments and configures logging.
func InitOptions() *Options {
    opt := &Options{
        opt:             getoptions.New(),
        RetryDelay:      10 * time.Second,
        MaxConnsPerHost: 8,
        RequestDelay:    500 * time.Millisecond,
        MetadataWorkers: 20,
    }

    setLogger(false, "")

    opt.opt.BoolVar(&opt.Help, "help", false, opt.opt.Alias("h"), opt.opt.Description("show help information"))
    opt.opt.BoolVar(&opt.Debug, "debug", false, opt.opt.Description("show more info"))
    opt.opt.BoolVar(&opt.SaveLog, "save-log", false, opt.opt.Description("save debug log info to file"))
    opt.opt.BoolVar(&opt.Version, "version", false, opt.opt.Alias("v"), opt.opt.Description("show version information"))
    opt.opt.StringVar(&opt.Input, "input", "", opt.opt.Alias("i"), opt.opt.Description("path to input tcia file"))
    opt.opt.StringVar(&opt.Output, "output", "./", opt.opt.Alias("o"), opt.opt.Description("Output directory for downloaded files"))
    opt.opt.StringVar(&opt.Proxy, "proxy", "", opt.opt.Alias("x"), opt.opt.Description("the proxy to use [http, socks5://user:passwd@host:port]"))
    opt.opt.IntVar(&opt.Concurrent, "processes", 2, opt.opt.Alias("p"), opt.opt.Description("start how many download at same time"))
    opt.opt.BoolVar(&opt.Meta, "meta", false, opt.opt.Alias("m"), opt.opt.Description("get Meta info of all files"))
    opt.opt.StringVar(&opt.Username, "user", "nbia_guest", opt.opt.Alias("u"), opt.opt.Description("username for control data"))
    opt.opt.BoolVar(&opt.Prompt, "prompt", false, opt.opt.Alias("w"), opt.opt.Description("input password for control data"))
    opt.opt.StringVar(&opt.Password, "passwd", "", opt.opt.Description("set password for control data in command line"))
    opt.opt.StringVar(&opt.TokenUrl, "token-url", TokenUrl, opt.opt.Description("the api url of login token"))
    opt.opt.StringVar(&opt.MetaUrl, "meta-url", MetaUrl, opt.opt.Description("the api url get meta data"))
    opt.opt.StringVar(&opt.ImageUrl, "image-url", ImageUrl, opt.opt.Description("the api url to download image data"))
    opt.opt.BoolVar(&opt.Force, "force", false, opt.opt.Alias("f"), opt.opt.Description("force re-download even if files exist"))
    opt.opt.BoolVar(&opt.SkipExisting, "skip-existing", false, opt.opt.Description("skip download if image file already exists"))
    opt.opt.IntVar(&opt.MaxRetries, "max-retries", 3, opt.opt.Description("maximum number of download retries"))
    opt.opt.IntVar(&opt.MaxConnsPerHost, "max-connections", 8, opt.opt.Description("maximum concurrent connections per host"))
    opt.opt.BoolVar(&opt.ServerFriendly, "server-friendly", false, opt.opt.Description("use extra conservative settings to avoid server issues"))
    opt.opt.BoolVar(&opt.NoMD5, "no-md5", false, opt.opt.Description("disable MD5 validation for downloaded files"))
    opt.opt.BoolVar(&opt.NoDecompress, "no-decompress", false, opt.opt.Description("keep downloaded files as ZIP archives (skip extraction)"))
    opt.opt.BoolVar(&opt.RefreshMetadata, "refresh-metadata", false, opt.opt.Description("force refresh all metadata from server (ignore cache)"))
    opt.opt.IntVar(&opt.MetadataWorkers, "metadata-workers", 20, opt.opt.Description("number of parallel metadata fetch workers"))

    if _, err := opt.opt.Parse(os.Args[1:]); err != nil {
        Logger.Fatal(err)
    }

    if opt.ServerFriendly {
        opt.Concurrent = 1
        opt.MaxConnsPerHost = 2
        opt.RetryDelay = 30 * time.Second
        opt.RequestDelay = 2 * time.Second
        opt.MetadataWorkers = 5
        Logger.Info("Server-friendly mode: Using extra conservative settings")
    }

    if opt.Debug || opt.SaveLog {
        setLogger(opt.Debug, filepath.Join(opt.Output, "progress.log"))
    }

    if opt.opt.Called("help") || len(os.Args) < 2 {
        fmt.Fprintf(os.Stderr, "%s", opt.opt.Help())
        os.Exit(1)
    }

    if !opt.NoMD5 && opt.NoDecompress {
        Logger.Fatal("MD5 validation (default) and --no-decompress are incompatible. Use --no-md5 with --no-decompress.")
    }

    if opt.TokenUrl != "" && opt.TokenUrl != TokenUrl {
        TokenUrl = opt.TokenUrl
        Logger.Infof("Using custom token url: %s", TokenUrl)
    }

    if opt.MetaUrl != "" && opt.MetaUrl != MetaUrl {
        MetaUrl = opt.MetaUrl
        Logger.Infof("Using custom meta url: %s", MetaUrl)
    }

    if opt.ImageUrl != ImageUrl && opt.ImageUrl != "" {
        ImageUrl = opt.ImageUrl
        Logger.Infof("Using custom image url: %s", ImageUrl)
    } else if !opt.NoMD5 {
        ImageUrl = "https://services.cancerimagingarchive.net/nbia-api/services/v2/getImageWithMD5Hash"
        Logger.Infof("Using MD5 validation endpoint (v2 with v1 fallback)")
    }

    if opt.Prompt {
        Logger.Infof("Please input password for %s: ", opt.Username)
        if _, err := fmt.Scanln(&opt.Password); err != nil {
            Logger.Fatalf("failed to scan prompt: %v", err)
        }
    }

    return opt
}
