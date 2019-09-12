package main

// network-secrets creates the yaml values for a new network

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"

	"github.com/filecoin-project/go-filecoin/commands"
	"github.com/filecoin-project/go-filecoin/gengen/util"
	"github.com/filecoin-project/go-filecoin/types"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"

	"gopkg.in/yaml.v2"
)

type Values struct {
	Image       string `yaml:"Image"`
	ImageTests  string `yaml:"ImageTests"`
	ImageFaucet string `yaml:"ImageFaucet"`
	Bootstrap   struct {
		NodePortStart int `yaml:"NodePortStart"`
	} `yaml:"Bootstrap"`
	Genesis struct {
		WalletAddress string `yaml:"WalletAddress"`
		MinerAddress  string `yaml:"MinerAddress"`
	} `yaml:"Genesis"`
	Secrets struct {
		GenesisFile string `yaml:"GenesisFile"`
		Wallet      string `yaml:"Wallet0Key"`
		Peer0Key    string `yaml:"Peer0Key"`
		Peer1Key    string `yaml:"Peer1Key"`
		Peer2Key    string `yaml:"Peer2Key"`
		Peer3Key    string `yaml:"Peer3Key"`
	} `yaml:"Secrets"`
}

func makepeerkey() ([]byte, string, error) {
	sk, _, err := crypto.GenerateKeyPair(crypto.RSA, 2048)
	if err != nil {
		return []byte{}, "", err
	}

	bs, err := crypto.MarshalPrivateKey(sk)
	if err != nil {
		return []byte{}, "", err
	}

	peerid, err := peer.IDFromPrivateKey(sk)
	if err != nil {
		return []byte{}, "", err
	}

	return bs, peerid.String(), nil
}

func main() {
	cfg := &gengen.GenesisCfg{
		Keys: 1,
		PreAlloc: []string{
			"100000000",
		},
		Miners: []*gengen.CreateStorageMinerConfig{
			{
				Owner:               0,
				NumCommittedSectors: 1,
			},
		},
		ProofsMode: types.LiveProofsMode,
	}

	gengen.ApplyProofsModeDefaults(cfg, cfg.ProofsMode == types.LiveProofsMode, true)

	var buffer bytes.Buffer

	info, err := gengen.GenGenesisCar(cfg, &buffer, 0)
	if err != nil {
		log.Fatal(err)
	}

	var values Values

	keyAddress, err := info.Keys[0].Address()
	if err != nil {
		log.Fatal(err)
	}

	var wallet commands.WalletSerializeResult
	wallet.KeyInfo = append(wallet.KeyInfo, info.Keys[0])

	walletBytes, err := json.Marshal(wallet)
	if err != nil {
		log.Fatal(err)
	}

	values.Genesis.MinerAddress = info.Miners[0].Address.String()
	values.Genesis.WalletAddress = keyAddress.String()
	values.Secrets.GenesisFile = base64.StdEncoding.EncodeToString(buffer.Bytes())
	values.Secrets.Wallet = base64.StdEncoding.EncodeToString(walletBytes)

	p0, _, err := makepeerkey()
	if err != nil {
		log.Fatal(err)
	}

	p1, id1, err := makepeerkey()
	if err != nil {
		log.Fatal(err)
	}

	p2, id2, err := makepeerkey()
	if err != nil {
		log.Fatal(err)
	}

	p3, id3, err := makepeerkey()
	if err != nil {
		log.Fatal(err)
	}

	values.Secrets.Peer0Key = base64.StdEncoding.EncodeToString(p0)
	values.Secrets.Peer1Key = base64.StdEncoding.EncodeToString(p1)
	values.Secrets.Peer2Key = base64.StdEncoding.EncodeToString(p2)
	values.Secrets.Peer3Key = base64.StdEncoding.EncodeToString(p3)

	d, err := yaml.Marshal(&values)
	fmt.Printf("%s\n", string(d))
	fmt.Println(id1)
	fmt.Println(id2)
	fmt.Println(id3)
}
