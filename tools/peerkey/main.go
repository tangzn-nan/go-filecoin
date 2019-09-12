package main

// peerkey will generate an RSA 2048 peerkey, writing it to the output specified by
// the -output flag. Instance of `<peerid>` will be replaced by the peerid in the
// output filename.

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

func main() {
	var output string = "<peerid>.peerkey"
	var silent bool = false

	flag.StringVar(&output, "output", output, "output of peerkey file")
	flag.BoolVar(&silent, "silent", silent, "suppress output of peerid to stdout")
	flag.Parse()

	sk, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		log.Fatal(err)
	}

	bs, err := crypto.MarshalPrivateKey(sk)
	if err != nil {
		log.Fatal(err)
	}

	peerid, err := peer.IDFromPrivateKey(sk)
	if err != nil {
		log.Fatal(err)
	}

	output = strings.ReplaceAll(output, "<peerid>", peerid.String())

	f, err := os.Create(output)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	if _, err := f.Write(bs); err != nil {
		log.Fatal(err)
	}

	if !silent {
		fmt.Println(peerid.String())
	}
}
