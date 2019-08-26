package main

import (
	"bigcache"
	rpccache "bigcache/ttrpc"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"

	"github.com/containerd/ttrpc"
)

const (
	// server version.
	version = "0.0.1"
	socket  = "bigcache-ttrpc-server"
)

var (
	port    int
	logfile string
	ver     bool

	cache  *bigcache.BigCache
	config = bigcache.Config{}
)

func init() {
	flag.BoolVar(&config.Verbose, "v", false, "Verbose logging.")
	flag.IntVar(&config.Shards, "shards", 1024, "Number of shards for the cache.")
	flag.IntVar(&config.MaxEntriesInWindow, "maxInWindow", 1000*10*60, "Used only in initial memory allocation.")
	flag.DurationVar(&config.LifeWindow, "lifetime", 100000*100000*60, "Lifetime of each cache object.")
	flag.IntVar(&config.HardMaxCacheSize, "max", 8192, "Maximum amount of data in the cache in MB.")
	flag.IntVar(&config.MaxEntrySize, "maxShardEntrySize", 500, "The maximum size of each object stored in a shard. Used only in initial memory allocation.")
	flag.IntVar(&port, "port", 9090, "The port to listen on.")
	flag.StringVar(&logfile, "logfile", "", "Location of the logfile.")
	flag.BoolVar(&ver, "version", false, "Print server version.")
}

func main() {
	if ver {
		fmt.Fprintf(os.Stdout, "BigCache teeny tiny RPC Server v%s", version)
		os.Exit(0)
	}

	var logger *logrus.Logger

	if logfile == "" {
		logger.SetOutput(os.Stdout)
	} else {
		f, err := os.OpenFile(logfile, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			logrus.Fatalf("Failed to log to file %s: %v", logfile, err)
		}
		logger.Out = f
	}

	var err error
	cache, err = bigcache.NewBigCache(config)
	if err != nil {
		logrus.Fatal(err)
	}

	logrus.Println("cache initialised.")
	s, err := ttrpc.NewServer(
		ttrpc.WithServerHandshaker(ttrpc.UnixSocketRequireSameUser()),
		ttrpc.WithUnaryServerInterceptor(serverIntercept),
	)
	if err != nil {
		logrus.Fatalln(err)
	}
	defer s.Close()
	rpccache.RegisterCacheServiceService(s, &bigcacheServer{})

	l, err := net.Listen("unix", socket)
	if err != nil {
		logrus.Fatalln(err)
	}
	defer func() {
		l.Close()
		os.Remove(socket)
	}()
	if err := s.Serve(context.Background(), l); err != nil {
		logrus.Fatalf("Error starting server: %v", err)
	}
}

type bigcacheServer struct{}

func (s bigcacheServer) Put(ctx context.Context, r *rpccache.PutRequest) (*empty.Empty, error) {
	if r.Key == "" {
		return nil, errors.New("ErrInputKeyNotFound")
	}
	if err := cache.Set(r.Key, []byte(r.Value)); err != nil {
		return nil, err
	}
	return nil, nil
}

func (s bigcacheServer) Delete(ctx context.Context, r *rpccache.DeleteRequest) (*empty.Empty, error) {
	if err := cache.Delete(r.Key); err != nil {
		if strings.Contains((err).Error(), "not found") {
			return nil, errors.New("ErrDoesNotExist")
		}
		return nil, err
	}
	return nil, nil
}

func (s bigcacheServer) Get(ctx context.Context, r *rpccache.GetRequest) (*rpccache.GetResponse, error) {
	entry, err := cache.Get(r.Key)
	if err != nil {
		errMsg := (err).Error()
		if strings.Contains(errMsg, "not found") {
			return nil, errors.New("ErrDoesNotExist")
		}
		return nil, err
	}
	return &rpccache.GetResponse{Value: entry}, nil
}

func serverIntercept(ctx context.Context, um ttrpc.Unmarshaler, i *ttrpc.UnaryServerInfo, m ttrpc.Method) (interface{}, error) {
	logrus.Infoln("server interceptor")
	dumpMetadata(ctx)
	return m(ctx, um)
}

func dumpMetadata(ctx context.Context) {
	md, ok := ttrpc.GetMetadata(ctx)
	if !ok {
		panic("no metadata")
	}
	if err := json.NewEncoder(os.Stdout).Encode(md); err != nil {
		panic(err)
	}
}
