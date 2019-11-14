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

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
	flag.BoolVar(&config.Verbose, "v", false, "verbose logging.")
	flag.IntVar(&config.Shards, "shards", 1024, "number of shards for the cache.")
	flag.IntVar(&config.MaxEntriesInWindow, "maxInWindow", 1000*10*60, "used only in initial memory allocation.")
	flag.DurationVar(&config.LifeWindow, "lifetime", 100000*100000*60, "lifetime of each cache object.")
	flag.IntVar(&config.HardMaxCacheSize, "max", 8192, "maximum amount of data in the cache in MB.")
	flag.IntVar(&config.MaxEntrySize, "maxShardEntrySize", 500, "the maximum size of each object stored in a shard. Used only in initial memory allocation.")
	flag.IntVar(&port, "port", 9090, "the port to listen on.")
	flag.StringVar(&logfile, "logfile", "", "location of the logfile.")
	flag.BoolVar(&ver, "version", false, "print server version.")
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
		return nil, status.Error(codes.Internal, (err).Error())
	}
	return nil, nil
}

func (s bigcacheServer) Delete(ctx context.Context, r *rpccache.DeleteRequest) (*empty.Empty, error) {
	if err := cache.Delete(r.Key); err != nil {
		if strings.Contains((err).Error(), "not found") {
			return nil, status.Error(codes.NotFound, (err).Error())
		}
		return nil, status.Error(codes.Internal, "Error removing the key")
	}
	return nil, nil
}

func (s bigcacheServer) Get(ctx context.Context, r *rpccache.GetRequest) (*rpccache.GetResponse, error) {
	entry, err := cache.Get(r.Key)
	if err != nil {
		errMsg := (err).Error()
		if strings.Contains(errMsg, "not found") {
			return nil, status.Error(codes.NotFound, errMsg)
		}
		return nil, status.Error(codes.Internal, errMsg)
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
