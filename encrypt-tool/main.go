package main

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/crypto/blowfish"
	"golang.org/x/term"
)

const (
	AES_CBC  = "AES/CBC/PKCS5Padding"
	AES_ECB  = "AES/ECB/PKCS5Padding"
	DES_CBC  = "DES/CBC/PKCS5Padding"
	DES_EDE3 = "DESede/CBC/PKCS5Padding"
	BLOWFISH = "Blowfish/CBC/PKCS5Padding"
)

type NewCipherBlockFunc func(key []byte) (cipher.Block, error)

type EncryptionModeFunc func(block cipher.Block, plainTextBlock []byte) []byte

type CipherAlgorithm struct {
	Lengths               []int
	LengthRange           map[string]int
	NewCipherBlockFuncPtr NewCipherBlockFunc
	EncryptionModeFuncPtr EncryptionModeFunc
}

var cipherAlgorithms = map[string]CipherAlgorithm{
	AES_CBC: {
		Lengths:               []int{128, 192, 256},
		LengthRange:           nil,
		NewCipherBlockFuncPtr: aes.NewCipher,
		EncryptionModeFuncPtr: generateCbcEncryptedTextBlock,
	},
	AES_ECB: {
		Lengths:               []int{128, 192, 256},
		LengthRange:           nil,
		NewCipherBlockFuncPtr: aes.NewCipher,
		EncryptionModeFuncPtr: generateEcbEncryptedTextBlock,
	},
	DES_CBC: {
		Lengths:               []int{64},
		LengthRange:           nil,
		NewCipherBlockFuncPtr: des.NewCipher,
		EncryptionModeFuncPtr: generateCbcEncryptedTextBlock,
	},
	DES_EDE3: {
		Lengths:               []int{192},
		LengthRange:           nil,
		NewCipherBlockFuncPtr: des.NewTripleDESCipher,
		EncryptionModeFuncPtr: generateCbcEncryptedTextBlock,
	},
	BLOWFISH: {
		Lengths:               nil,
		LengthRange:           map[string]int{"min": 32, "max": 448},
		NewCipherBlockFuncPtr: BlowfishNewCipherWrapper,
		EncryptionModeFuncPtr: generateCbcEncryptedTextBlock,
	},
}

const EncryptToolVersionString = "1.0"

// BlowfishNewCipherWrapper
//
//	Need to have a wrapper function for the Blowfish cipher as it returns a Cipher object which needs to be cast to a
//	cipher.Block interface.
func BlowfishNewCipherWrapper(key []byte) (cipher.Block, error) {
	return blowfish.NewCipher(key)
}

func main() {

	argVector := os.Args
	argNumber := len(argVector)

	if argNumber < 2 {
		usage()
		os.Exit(1)
	}

	switch argVector[1] {
	case "createkey":
		if argNumber != 5 {
			usage()
			os.Exit(1)
		}
		intVal, err := strconv.Atoi(argVector[3])
		if err != nil {
			slog.Error("Invalid length: ", argVector[3])
			os.Exit(1)
		}
		createKey(argVector[2], intVal, argVector[4])
		break
	case "encryptvalue":
		if argNumber != 3 {
			usage()
			os.Exit(1)
		}
		encryptValue(argVector[2])
		break
	case "help":
		usage()
		os.Exit(0)
	}
}

func usage() {
	fmt.Println("Encrypt Tool Version: ", EncryptToolVersionString)
	fmt.Println()
	fmt.Println("Usage: ", os.Args[0], " [createkey <algorithm> <length> <keypath> | encryptvalue <keypath> | help]")
	fmt.Println()
	fmt.Println("Supported algorithms:")
	for key := range cipherAlgorithms {
		if cipherAlgorithms[key].Lengths != nil {
			fmt.Println("    ", key, " (", cipherAlgorithms[key].Lengths, " bits)")
		} else if cipherAlgorithms[key].LengthRange != nil {
			fmt.Println(
				"    ",
				key,
				" (",
				cipherAlgorithms[key].LengthRange["min"],
				"-",
				cipherAlgorithms[key].LengthRange["max"],
				" bits)",
			)
		}
	}
}

func createKey(algorithm string, numBits int, keypath string) {
	algorithmVal, ok := cipherAlgorithms[algorithm]
	if !ok {
		slog.Error(fmt.Sprintf("Invalid algorithm: %s", algorithm))
		usage()
		os.Exit(1)
	}

	if algorithmVal.Lengths != nil {
		if !slices.Contains(algorithmVal.Lengths, numBits) {
			slog.Error(fmt.Sprintf(
				"Invalid length for algorithm: %s. Valid lengths are %v",
				algorithm,
				algorithmVal.Lengths,
			))
			os.Exit(1)
		}
	} else if algorithmVal.LengthRange != nil {
		if numBits < algorithmVal.LengthRange["min"] || numBits > algorithmVal.LengthRange["max"] {
			slog.Error(fmt.Sprintf(
				"Invalid length for algorithm: %s. Valid lengths are between %d and %d",
				algorithm,
				algorithmVal.LengthRange["min"],
				algorithmVal.LengthRange["max"],
			))
			os.Exit(1)
		}
	}

	slog.Info(fmt.Sprintf("Generating %d-bit key for algorithm %s", numBits, algorithm))
	numBytes := numBits / 8
	byteArray := make([]byte, numBytes)
	_, err := rand.Read(byteArray)

	if err != nil {
		slog.Error(fmt.Sprintf("Failed to generate key: %s", err))
		os.Exit(1)
	}

	algorithmNameLen := len(algorithm) + 1
	base64EncodingLen := base64.StdEncoding.EncodedLen(len(byteArray))
	encodedKey := make([]byte, algorithmNameLen+base64EncodingLen)
	copy(encodedKey[:algorithmNameLen], algorithm+":")
	base64.StdEncoding.Encode(encodedKey[algorithmNameLen:], byteArray)

	err = os.WriteFile(keypath, encodedKey, 0600)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to write key to file: %s", err))
		os.Exit(1)
	}

	slog.Info(fmt.Sprintf("Key written to file %s", keypath))
}

func encryptValue(keypath string) {
	slog.Info("Encrypting value")

	key, cipherType, err := readKeyAndCipherType(keypath)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to read key file: %s", err))
		os.Exit(1)
	}

	algorithmVal, ok := cipherAlgorithms[cipherType]
	if !ok {
		slog.Error(fmt.Sprintf("Invalid algorithm: %s", cipherType))
		usage()
		os.Exit(1)
	}

	block, err := algorithmVal.NewCipherBlockFuncPtr(key)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to create cipher block: %s", err))
		os.Exit(1)
	}

	plainTextValue := readSensitiveData()
	plainTextBlock := initialiseTextBlock(plainTextValue, block.BlockSize())
	encryptedTextBlock := generateEncryptedTextBlock(
		block,
		plainTextBlock,
		algorithmVal.EncryptionModeFuncPtr,
	)

	fmt.Println()
	fmt.Println("Your encrypted value is: ")
	fmt.Println()
	fmt.Println(string(encryptedTextBlock))
	fmt.Println()
}

func readKeyAndCipherType(keypath string) ([]byte, string, error) {
	keyData, err := os.ReadFile(keypath)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to read key file: %s", err))
		os.Exit(1)
	}
	parts := strings.Split(string(keyData), ":")
	lenParts := len(parts)
	if lenParts < 2 && lenParts > 3 {
		return nil, "", fmt.Errorf("invalid key file format")
	}

	var keyBase64 string
	cipherType := parts[0]
	if lenParts == 2 {
		keyBase64 = parts[1]
	} else {
		// Handle keys generated by dsetool
		keyBase64 = parts[2]
	}

	keyBytes, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to decode key: %s", err))
		os.Exit(1)
	}

	return keyBytes, cipherType, nil
}

func readSensitiveData() []byte {
	for {
		fmt.Println()
		fmt.Print("Enter value to encrypt: ")
		byteInputAttempt1, err := term.ReadPassword(syscall.Stdin)
		fmt.Println()

		if err != nil {
			slog.Error("Failed to read input: ", err)
			os.Exit(1)
		}

		fmt.Print("Enter the value again to confirm: ")
		byteInputAttempt2, err := term.ReadPassword(syscall.Stdin)
		fmt.Println()

		fmt.Println()
		if err != nil {
			slog.Error("Failed to read input: ", err)
			os.Exit(1)
		}

		if string(byteInputAttempt1) == string(byteInputAttempt2) {
			return byteInputAttempt1
		} else {
			fmt.Println("Values entered are different. Please try again.")
		}
	}
}

func initialiseTextBlock(plainTextValue []byte, blockSize int) []byte {
	var plainTextBlock []byte
	plainTextLen := len(plainTextValue)

	if plainTextLen%blockSize != 0 {
		extendBlockLen := blockSize - (plainTextLen % blockSize)
		plainTextBlock = make([]byte, plainTextLen+extendBlockLen)
		copy(plainTextBlock[plainTextLen:], bytes.Repeat([]byte{uint8(extendBlockLen)}, extendBlockLen))
	} else {
		plainTextBlock = make([]byte, plainTextLen)
	}

	copy(plainTextBlock, plainTextValue)

	return plainTextBlock
}

func generateInitializationVector(blockSize int) []byte {
	iv := make([]byte, blockSize)
	_, err := rand.Read(iv)
	if err != nil {
		slog.Error("Failed to generate initialization vector: ", err)
		os.Exit(1)
	}

	return iv
}

func generateEncryptedTextBlock(block cipher.Block, plainTextBlock []byte, modeFunc EncryptionModeFunc) []byte {
	encryptedTextBlock := modeFunc(block, plainTextBlock)

	encodedBlock := make([]byte, base64.StdEncoding.EncodedLen(len(encryptedTextBlock)))
	base64.StdEncoding.Encode(encodedBlock, encryptedTextBlock)

	return encodedBlock
}

func generateCbcEncryptedTextBlock(block cipher.Block, plainTextBlock []byte) []byte {
	slog.Info("Encrypting value using Cipher Block Chaining (CBC) mode")

	iv := generateInitializationVector(block.BlockSize())
	cipherTextBlockLen := block.BlockSize() + len(plainTextBlock)
	cipherTextBlock := make([]byte, cipherTextBlockLen)
	copy(cipherTextBlock[:block.BlockSize()], iv)

	cbc := cipher.NewCBCEncrypter(block, iv)
	cbc.CryptBlocks(cipherTextBlock[block.BlockSize():], plainTextBlock)

	return cipherTextBlock
}

func generateEcbEncryptedTextBlock(block cipher.Block, plainTextBlock []byte) []byte {
	slog.Info("Encrypting value using Electronic Code Book (ECB) mode")

	plainTextBlockLen := len(plainTextBlock)
	encryptedData := make([]byte, plainTextBlockLen)
	blockSize := 16
	for bs, be := 0, blockSize; bs < plainTextBlockLen; bs, be = bs+blockSize, be+blockSize {
		block.Encrypt(encryptedData[bs:be], plainTextBlock[bs:be])
	}

	return encryptedData
}
