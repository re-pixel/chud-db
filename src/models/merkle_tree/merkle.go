package merkle_tree

import (
	"crypto/sha256"
	"fmt"
	"math"
)

type MerkleTree struct {
	lvlValues []string
	maxLevels int
}

func InitializeMerkleTree(leafCount int) *MerkleTree {
	if leafCount <= 0 {
		leafCount = 1
	}
	maxLevels := int(math.Ceil(math.Log2(float64(leafCount))))
	if maxLevels == 0 {
		maxLevels = 1
	}
	
	return &MerkleTree{
		lvlValues: make([]string, maxLevels),
		maxLevels: maxLevels,
	}
}

func (mt *MerkleTree) hashValues(left, right string) string {
	combined := left + right
	hash := sha256.Sum256([]byte(combined))
	return fmt.Sprintf("%x", hash)
}

func (mt *MerkleTree) AddLeaf(value string) {
	hash := sha256.Sum256([]byte(value))
	currentHash := fmt.Sprintf("%x", hash)
	
	level := 0
	
	for level < mt.maxLevels {
		if mt.lvlValues[level] == "" {
			mt.lvlValues[level] = currentHash
			break
		} else {
			leftHash := mt.lvlValues[level]
			combinedHash := mt.hashValues(leftHash, currentHash)
			
			mt.lvlValues[level] = ""
			
			currentHash = combinedHash
			level++
			
			if level >= mt.maxLevels {
				break
			}
		}
	}
}

func (mt *MerkleTree) GetRoot() string {
	if mt.lvlValues[mt.maxLevels-1] != "" {
		return mt.lvlValues[mt.maxLevels-1]
	}
	
	for mt.lvlValues[mt.maxLevels-1] == "" {
		mt.AddLeaf("") 
	}
	
	return mt.lvlValues[mt.maxLevels-1]
}

// GetRootBytes vraća root hash kao byte array
func (mt *MerkleTree) GetRootBytes() []byte {
	rootHash := mt.GetRoot()
	return []byte(rootHash)
}