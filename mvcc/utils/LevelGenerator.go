package utils

import (
	"math/rand"
	"time"
)

type LevelGenerator struct {
	maxLevel   uint8
	skipFactor int //use to calculate probability of skipping a level
	random     *rand.Rand
}

func NewLevelGenerator(maxLevel uint8) *LevelGenerator {
	randObject := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &LevelGenerator{maxLevel: maxLevel, skipFactor: 2, random: randObject}
}

func (levelGenerator LevelGenerator) Generate() uint8 {
	level := uint8(1)
	newRandomNumber := levelGenerator.random.Float64() //generates a float in range [0.0, 1.0)
	for level < levelGenerator.GetMaxLevel() && newRandomNumber < 1.0/float64(levelGenerator.skipFactor) {
		level++
		newRandomNumber = levelGenerator.random.Float64()
	}
	return level
}

func (levelGenerator LevelGenerator) GetMaxLevel() uint8 {
	return levelGenerator.maxLevel
}
